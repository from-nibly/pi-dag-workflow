<!-- generated-by: pi-dag-workflow/project-model; view: SPEC-structured-brainstorming; contract: 1; input: sha256:c416254464fd769dc08f760e4c548e72fde875286a4b474f2972a67c711fe842 -->

# Structured brainstorming behavior

Retained research, interaction, question-quality, prototype, and validation behavior migrated as commitments.

## Candidate behavioral contracts

<a id="obj-com-contextual-working-sets"></a>

### Contextual working sets

Turns may address many related questions without numeric or single-decision limits.

**Rationale.** Operant sessions show broad contextually continuous review supports deep sparse feedback.

<a id="obj-com-question-distinction"></a>

### Questions are durable and distinct from discussion points

Workstreams and focus sessions scope attention; durable questions/tensions represent unresolved meaning; ephemeral review turns present exact hash-bound points; explicit outcomes update the linked proposal, decision, commitment, or question state. Retiring a review point never deletes the underlying model object.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-no-resurrection"></a>

### Do not silently resurrect settled review points

Remove resolved presentation points from the active review turn. If new evidence challenges governing direction, create a linked reconsideration question and mark the effective object Under review; do not erase, silently rewrite, or casually re-present accepted authority.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-snapshot-only"></a>

### Use one current model snapshot without event sourcing

Use one tracked `project-model/model.json` current snapshot as semantic authority, with Git history rather than an application event log. Focus sessions and one replaceable previous-review snapshot are ephemeral presentation/baseline state and contain no unique project meaning.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-derived-views"></a>

### Treat generated views as non-authoritative projections

Generated specifications and oversight surfaces are projections of selected project-model objects and are never independent authority. Tracked specs regenerate deterministically from model-owned prose; review turns and other oversight views remain ephemeral by default.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-single-agent"></a>

### Use single-agent model brainstorming

Brainstorming is single-agent and single-writer. Concurrent use of one focus receives no lock/merge UX, but canonical model writes use Pi's file-mutation queue and review outcomes enforce object-level semantic-hash freshness.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-inline-rationale"></a>

### Keep rationale with authoritative model content

Keep rationale in the authoritative model object beside the intent, decision, scenario, or commitment it explains; deterministic specification projections render that rationale near the governing prose.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-prototypes-directory"></a>

### Use slugged prototype directories

Retained prototypes live under spec/prototypes/<slug>/ and include local explanatory Markdown plus their non-Markdown assets/code.

**Rationale.** Stable readable slugs make prototypes easy to reference from spec Markdown.

<a id="obj-com-markdown-links"></a>

### Use Markdown links and repository-root comments

Generated specs use stable Markdown links and anchors calculated from projection metadata. Hand-authored prototype READMEs/assets may link to generated specs or stable model IDs; do not persist machine-local presentation-session URLs as durable references.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-markdown-only"></a>

### Keep generated specifications in Markdown

Tracked specification views under `spec/` are generated Markdown, not authority. Prototype READMEs/assets remain hand-authored evidence and may include non-Markdown files; authoritative semantic prose remains in `project-model/model.json`.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-openspec-conventions-only"></a>

### Adopt OpenSpec-inspired spec conventions only

Domain-oriented Markdown organization may inform generated specification views, but the project model, six tools, projection metadata, and mixed-initiative loop remain project-owned; do not add OpenSpec runtime/package/schema authority.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-no-openspec-changes"></a>

### Do not adopt OpenSpec changes

Do not create openspec/changes, delta specs, OpenSpec task artifacts, or adapters around OpenSpec sync/archive.

**Rationale.** The change lifecycle would introduce a second planning and archive model without enough additional value for this workflow.

<a id="obj-com-keep-spec-root"></a>

### Keep generated specifications under spec/

Keep tracked generated specification views under `spec/`; authoritative semantic content lives in `project-model/model.json`, and projection paths/order live in non-semantic project metadata.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-structured-discovery-loop"></a>

### Use an alternating structured discovery loop

Operate model brainstorming as Explore → Consolidate → Stress-test → Commit → Project, repeating or moving backward as useful. Exploration writes non-authoritative model objects immediately; review turns separate For awareness from exact Decisions needed; no promotion step exists.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-bounded-autonomous-bursts"></a>

### Explore autonomously through the selected focus frontier

Explore autonomously within the selected focus and initiate a materiality-based review turn when a coherent cluster needs product judgment, evidence changes framing, a preference blocks progress, continuing risks drift, or the user requests review. After a frontier review is resolved, automatically research and present the next material frontier without waiting for another chat prompt. Continue until no supported material frontier remains, the user pauses or redirects, or progress is blocked on unresolved user input. Use no fixed tool, time, object, or token quota, and do not invent questions merely to claim exhaustion.

**Rationale.** The user wants a continuous formal brainstorming loop rather than manually requesting each next frontier, while preserving the existing prohibition on fabricated exhaustion sweeps.

<a id="obj-com-user-selects-tangents"></a>

### The user controls focus scope

The user controls the active focus by selecting or changing workstreams/bounded scopes. The agent may autonomously research and add sourced non-authoritative objects within that focus, surface cross-scope implications, and propose focus changes, but does not silently redefine product direction or scope.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-optional-reasoned-recommendations"></a>

### Recommendations are optional and reasoned

The agent may recommend an option during convergence but is not required to. Every recommendation must include its rationale, mark the option as (Recommended), retain explained alternatives, and permit Other.

**Rationale.** Some decisions have enough evidence for a useful recommendation while others should remain unranked; unsupported recommendations create false authority.

<a id="obj-com-unrestricted-tangent-multiselect"></a>

### Allow multi-workstream focus

A focus session may select one or more workstreams or bounded scopes without an arbitrary count limit. The agent does not silently change the selected focus, while cross-workstream links and newly discovered non-authoritative objects remain visible.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-no-tangent-selection-lease"></a>

### Focus selection has no lease

A focus session's selected workstreams remain active until the user redirects or stops the focus; selection is an attention boundary, not an authority lease. Reload, resume, fork, and clone reactivate the exact linked focus after validation.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-integrate-clear-preserve-unclear"></a>

### Resolve clear outcomes and preserve unclear responses

`dag_model_resolve_review` applies each explicit, fresh, hash-matching outcome independently. Omitted, ambiguous, contradictory, or stale points remain unresolved and regenerate against current model content.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-user-requests-prototypes"></a>

### Create prototypes only on user request

The agent may recommend or discuss a prototype, but creates one only after the user explicitly requests it.

**Rationale.** Prototype creation is a user-directed brainstorming activity rather than an autonomous agent choice.

<a id="obj-com-prototypes-start-under-spec"></a>

### Create prototypes under spec from the start

Every requested prototype begins under spec/prototypes/<easy-reference-slug>/; do not stage prototype files under .ai.

**Rationale.** A single tracked location avoids a separate staging and retention lifecycle.

<a id="obj-com-prototype-readme"></a>

### Each prototype has a README

Each prototype slug directory contains README.md explaining its purpose, operation, limitations, findings, and links to related specs.

**Rationale.** README.md explains the prototype as an artifact while spec.md remains associated with canonical functional spec directories.

<a id="obj-com-curate-prototypes"></a>

### Mutate freely and retain only useful prototypes

Prototype files may change or be removed while exploratory. Retain artifacts that remain behaviorally useful, and record any still-material finding as a discovery/evidence item or other appropriate model object before deleting a failed, superseded, or noisy artifact.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-prototype-bounded-behavioral-reference"></a>

### Prototype is a bounded behavioral reference

A prototype is hand-authored evidence with an explicit README scope, not governing authority. Record material findings as discoveries/evidence and accept any resulting intent, decision, scenario, or commitment through normal model authority rules; accidental prototype details do not become requirements.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-immediate-spec-prototype-ambiguity"></a>

### Treat prototype/spec mismatch as a discovery

Treat a mismatch between a prototype and a generated specification/model commitment as a sourced discovery and divergence. Neither artifact mutates authority: reconcile the finding into proposed/reopened model objects and request human judgment only when accepted direction may change.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-prototype-readme-execution-and-scope"></a>

### README documents execution and scope boundaries

Every prototype README provides a runnable or viewable entrypoint, expected observations, demonstrated behavior, out-of-scope behavior, and the boundary where its definitions end. Partial samples explicitly state the omitted cases they do not flesh out.

**Rationale.** Agents need both a way to exercise the prototype and an explicit statement of what conclusions it supports.

<a id="obj-com-retain-useful-prototypes"></a>

### Retain prototypes while behaviorally useful

Retain a prototype while it uniquely improves understanding or validation. It may be removed after material findings are represented in the project model or other durable evidence and its unique behavioral role is gone.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-flexible-requirements-and-scenarios"></a>

### Use requirements and scenarios where useful

Accepted model objects may use explicit requirements and concrete scenarios when they improve precision, alongside rationale, invariants, flows, constraints, diagrams, and links. Generated specs project that prose without requiring OpenSpec syntax.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-short-linked-support-summaries"></a>

### Link canonical object placements instead of duplicating prose

Each accepted object's full governing body has one canonical generated placement. Other generated specs use projector-owned links and short navigation summaries rather than duplicating normative prose.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-prototype-specmd-exception"></a>

### Prototype slug directories are exempt from spec.md

Prototype slug directories use hand-authored `README.md` as their evidence entry point. The projector protects these directories and never generates or overwrites their READMEs/assets.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-compact-resume-orientation"></a>

### Show a compact current-state orientation on resume

On resume, show the selected workstreams/focus, Current understanding, any active review turn, the unresolved question/tension frontier, material model delta since the previous-review baseline, and governing items under review. Load deeper model slices or generated specs only as needed.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-refresh-selected-focus-on-resume"></a>

### Refresh evidence for the selected focus on resume

Before substantive resumed exploration, re-read the model slices, generated specs, code/tests, and external evidence relevant to the selected workstreams. Record changed facts as discoveries/divergence and never silently rewrite accepted objects; do not require a repository-wide scan.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-continuous-tangent-discovery"></a>

### Continuously record useful discoveries

Throughout exploration, record coherent new findings as sourced discoveries and triage their possible impact into evidence, assumptions, questions, tensions, proposals, scenarios, commitments, or a proposed workstream change. Do not assume the initial focus is complete or invent objects to satisfy a quota.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-show-tangents-at-review-checkpoint"></a>

### Show material discoveries at the next review turn

Record a coherent finding immediately as a non-authoritative discovery. Present it under For awareness at the next materiality-based review turn; initiate review sooner when it invalidates current framing or creates material drift.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-tangent-candidate-context"></a>

### Give discoveries source, scope, and possible impact

A discovery or proposed workstream change records the finding/evidence that exposed it, the uncertainty or research need, its repository/workstream scope, and its possible model impact. Do not fabricate downstream questions before evidence supports them.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-agent-semantically-deduplicates-tangents"></a>

### Deduplicate semantic model objects

Before adding a discovery, question, tension, proposal, or workstream, compare its meaning with current model objects. Reuse or link substantial overlap, use typed supersession where meaning is replaced, and ask the user only when a material distinction requires product judgment.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-no-forced-exhaustion-sweep"></a>

### Do not force an exhaustion sweep or invented tangents

Add discoveries or other non-authoritative model objects only when supported and useful. At a materiality-based review turn, report material additions or state that none were found; do not fabricate branches or claim completeness.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-agent-judges-tangents-tools-validate-structure"></a>

### Agent judges meaning; tools validate model structure

The agent judges whether a finding merits a discovery, question, tension, proposal, or workstream change and identifies semantic overlap. Tools validate stable identity, type-specific state, scope, references, hashes, and allowed transitions; they do not score research value or require fabricated discoveries.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-defer-toon"></a>

### Do not adopt TOON now

Keep `project-model/model.json` and model-tool inputs as JSON with compact domain-shaped projections. Do not require TOON in V1; consider alternate encodings only after dogfooding identifies a measured bottleneck.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-judgment-structure-direction-transport"></a>

### Use the judgment, structure, direction, and transport ownership matrix

Agents own research, synthesis, semantic classification, and response interpretation. Tools own model schema/invariants, atomic writes, content hashes, receipts, references, type-specific transitions, and deterministic projections. Humans own outcomes, priorities, values, focus, and acceptance or replacement of governing direction.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-four-layer-tool-output"></a>

### Separate datastore, model content, tool details, and user rendering

Separate the authoritative project model, narrow model-facing tool projections/receipts, ephemeral focus and review presentation state, and generated user-facing views. Routine tool results do not return the complete model or generated documents; review is the bounded exception that returns exact hash-bound context and options.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-context-before-review-decisions"></a>

### Review points explain context before asking for a decision

Each substantive review point first provides concise context and explanation of the uncertainty, implications, and relevant evidence. It then states the clear user-facing question and explained decision options. Avoid both context-free questionnaires and duplicated long-form specification text.

**Rationale.** Bare questions and options do not provide enough evidence or reasoning for high-quality user judgment.

<a id="obj-com-mechanical-inference-only"></a>

### Infer mechanics through typed defaults and explicit references

Tools generate and validate mechanical fields—typed IDs, timestamps, semantic hashes, receipts, derived reverse links, projection links, and explicitly requested type-specific transitions. Agents author and classify meaning, rationale, recommendations, user interpretation, and semantic relationships; tools do not infer them from prose.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-research-classify-before-ask"></a>

### Classify input and exhaust available evidence before asking

First distinguish user requirements/corrections from unresolved decisions, inspect relevant specs/code/research, and identify whether the remaining uncertainty requires user preference or authority. Integrate established direction directly instead of turning it into a choice.

**Rationale.** Several weak dogfood questions asked the user to reconfirm what they had just established or externalized questions the repository/renderer boundary could answer.

<a id="obj-com-question-worthiness"></a>

### Use user attention for meaningful product judgment

Before asking, exhaust relevant repository evidence and perform web research when the user's framing shows uncertainty, unfamiliarity, conflicting possibilities, or an explicit research suggestion. Ask the user only when the remaining answer requires judgment about direction, scope, purpose, methodology, style, or another material product choice. Do not ask for facts, mechanical details, or reversible implementation choices that available evidence and established conventions can resolve.

**Rationale.** Software can be built with many languages, tools, interfaces, methodologies, and styles; those choices require user intent. Repository facts and routine mechanics do not deserve the same scarce attention.

<a id="obj-com-question-context"></a>

### Explain the decision background before asking for a choice

Every Decisions-needed point uses a mandatory context core plus adaptively selected supporting context. It must establish what needs to be decided, why the decision exists or matters now, the agent's recommendation and rationale, and the exact input needed from the user. Prompts actively encourage the agent to add relevant current behavior, the observed insufficiency or trigger, what would change and remain unchanged, constraints or prior decisions, realistic consequences, remaining uncertainty, and materially different option effects whenever those details help the user make an informed choice. Define specialized terms in plain language and distinguish correctness protection from optional hardening. Do not mechanically render every possible section, but do not present only a description of the recommended option or assume the user remembers earlier research.

**Rationale.** The user needs a reliable floor of decision context and visibility into the relevant project model without replacing dense prose with an equally rigid, exhaustive template.

<a id="obj-com-real-options-only"></a>

### Offer only real alternatives and allow open responses

Use structured options only for plausible, materially distinct choices. Do not force a fixed option count. If the space is not understood, ask an open question or propose research. Recommendations remain optional, reasoned, clearly marked, and accompanied by explained alternatives and Other.

**Rationale.** Invented alternatives anchor the user and disguise settled requirements as decisions; structured choices help only when the option space is real.

<a id="obj-com-question-corrections"></a>

### Treat corrections as evidence and preserve prior user intent

Classify an explicit user correction under normal authority rules. Correct a non-authoritative agent mistake directly; if it conflicts with accepted direction, preserve the governing object, create a linked contradiction/reconsideration question, and require explicit suspension, retirement, or supersession before its governing effect changes.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-understanding-in-resolve-review"></a>

### Synthesize Current understanding inside resolve_review

When resolved review outcomes materially change the model, atomically replace Current understanding in the same resolution transaction using the resulting object IDs/hashes. The synthesis remains explicitly agent-authored and non-authoritative.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-meaningful-semantic-refresh"></a>

### Use formal review projections instead of separate acknowledgements

Replace Current understanding after a material model change, accepted correction, contradiction resolution, or research reframing—not for navigation or unchanged conversation. Retain only the latest non-authoritative synthesis and its source object IDs/hashes. After resolving user direction, do not produce a distinct acknowledgement; continue to the next formal review, whose Current understanding and Model delta communicate the result.

**Rationale.** The next formal review already contains the persistent synthesis and delta, so a separate acknowledgement would duplicate presentation.

<a id="obj-com-initial-orientation-proof"></a>

### Present Initial understanding before the first Decisions-needed review

Before the first decisions-needed review, present an Initial understanding synthesis grounded in user input, relevant model objects, repository/code evidence, known constraints, and explicit uncertainty. Store it as agent-authored, non-authoritative Current-understanding metadata rather than as a renderer component or acceptance request.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-single-current-proof"></a>

### Keep one structured mutable current proof

Keep one latest project-level Current understanding synthesis in tracked metadata. It is agent-authored, non-authoritative, carries source object IDs/hashes, and is replaced rather than accumulated as review history. Its Markdown must make the goal, relevant current state or mechanism, and governing accepted direction easy to find. Add unresolved frontier, boundaries, constraints, evidence, risks, or other sections when they materially help the user verify alignment. Headings may adapt to the subject; Current understanding describes present state rather than duplicating the separate model-delta or acknowledgement receipt.

**Rationale.** A mandatory semantic core makes the synthesis auditable, while adaptive headings preserve useful judgment and avoid template noise.

<a id="obj-com-structural-proof-validation"></a>

### Mechanically validate delivery and provenance, not comprehension

Require agent guidance to produce scannable Markdown and contextual synthesis for question briefs and Current understanding. Deterministic tools and tests validate that this guidance is delivered, Current understanding is nonempty when present, source object IDs/hashes are valid and current, and headings, lists, emphasis, and other supported Markdown render safely. Do not reject semantic output based on fixed section names, heading counts, or checklist completeness, and do not claim that structural conformance proves comprehension. Agent judgment and later field-grounded evaluation assess whether the synthesis selected, connected, and explained the facts that matter.

**Rationale.** This catches delivery and rendering regressions while preserving the agent-authored synthesis that serves as the actual alignment proof.

<a id="obj-com-mode-system-prompt-plus-kickoff"></a>

### Use mode-specific system guidance plus a compact kickoff

While model brainstorming is active, inject the operational behavior contract and send one compact kickoff naming the repository model, exact focus session, selected workstreams/seed, orientation, and immediate action. Dynamically activate the six model tools without duplicating the contract in verbose tool guidance.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-retire-grillme"></a>

### Retire GrillMe in favor of structured brainstorming

At cutover, remove or unregister GrillMe tools, editor, prompts, state handling, documentation, and production registration. Model brainstorming is the sole active DAG-owned research/review workflow; no compatibility alias or GrillMe-state migration is required.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-evidence-stack"></a>

### Use automated verification as the current production validation contract

Validate the complete brainstorming-first production slice with repeatable automated tests runnable locally or in CI, including schema/reference invariants, atomic model mutation, content-hash authority, projection drift/safety, focus lifecycle, and Pi tool activation. Renderer/browser contracts remain outside V1; sustained-use and model-scored semantic quality are not release gates.

**Rationale.** Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior.

<a id="obj-com-semantic-evaluation"></a>

### Separate agent prompt evaluation from production validation

Do not include agent-prompt quality scoring, LLM judges, or semantic transcript thresholds in the current production validation contract. Use the workflow for several weeks, preserve useful observations, and design a separate prompt-evaluation exercise later. Automated production tests may verify structural prompt delivery and deterministic outcomes but must not claim to prove question or comprehension quality.

**Rationale.** A useful prompt-evaluation corpus and rubric should be grounded in real failure patterns. Building it before sustained use risks optimizing for speculative fixtures and making unstable model behavior look like a product gate.

<a id="obj-com-token-budget"></a>

### Keep tool interaction sensibly efficient without token budgets

Do not establish hard byte/token caps, a canonical tokenizer, or percentage regression gates. Apply obvious structural efficiency: send IDs/references instead of repeated stored objects; let operations accept narrow domain changes rather than whole-state JSON documents; batch related mutations when useful; avoid echoing authored prose; return compact projection-shaped receipts; and use file/artifact references when genuinely large content is already external. Preserve meaningful context and straightforward tool ergonomics rather than adding indirection solely to reduce tokens. Use prototype and production dogfooding to identify concrete waste before further optimization.

**Rationale.** The important opportunity is removing token usage that provides no value, not optimizing against arbitrary numbers. Sensible schemas and projections capture the major gains without making calls brittle or obscuring semantic content.

<a id="obj-com-build-bounded-lavish-turn-renderer-prototype"></a>

### Build the bounded Lavish turn-renderer prototype

Implement `spec/prototypes/lavish-turn-renderer/` as a fixture-driven vertical prototype of the settled whole-turn renderer. It will include a versioned review-turn projection, package-owned self-contained shell, standard sparse decision controls, trusted rich-context examples, deterministic artifact lifecycle, bounded feedback normalization, fake Lavish open/poll/end scenarios, and one opt-in live exercise. It will not register production tools or mutate project-model authority. Use dogfood findings to refine the production adapter contract.

**Rationale.** The broad product direction is settled and the remaining uncertainty is best reduced through a concrete bounded artifact.
