# Production validation

> **Migration evidence:** The general validation principles remain useful, but legacy brainstorm-tool specifics are superseded by the [mixed-initiative project model](../mixed-initiative-project-model/spec.md) and its production tests.

## Scope

The current production-validation contract covers behavior that can be verified through repeatable automated tests running locally or in CI. It does not use manual release exercises, sustained-use judgment, LLM-as-judge scores, or semantic prompt-quality thresholds as delivery gates.

Automatable evidence may include:

- unit and static tests;
- schema/reference invariant tests;
- temp-project integration scenarios;
- fake Pi API, context, UI, and tool-activation tests;
- concurrent mutation tests;
- deterministic browser automation;
- mocked renderer submissions and failures;
- deterministic artifact and layout checks;
- compact input/output shape assertions.

**Rationale:** mechanical behavior can be verified before delivery. Whether the agent's research, questions, recommendations, and Current understanding are genuinely useful requires sustained real use and should not be disguised as an early deterministic gate.

## Agent prompt evaluation

Agent prompt quality is a separate future exercise. After several weeks of structured-brainstorm use, observed successes and failures may support a versioned transcript corpus, rubric, model-variance study, and prompt-regression workflow.

Current automated tests may verify that mode guidance is injected at the correct lifecycle points and that deterministic semantic outcomes are represented correctly. They do not claim to prove question quality, research sufficiency, tangent usefulness, or comprehension quality.

## Pi adapter prototype

The user-requested [Brainstorm Pi adapter prototype](../prototypes/brainstorm-pi-adapter/README.md) is required behavioral evidence before production integration. Its deterministic temp-project scenario covers:

- interactive and headless new/resume selection;
- selective brainstorm tool activation/suspension;
- mode-specific system guidance;
- all five adapter shapes;
- per-snapshot mutation serialization;
- malformed/missing snapshot handling;
- Current understanding lifecycle;
- renderer failure leaving a review active;
- renderer submissions staying outside semantic state;
- semantic resolution and review retirement;
- promotion prepare → edit → record;
- GrillMe absence.

The prototype remains independent from final production code and from the object-tools domain prototype. Production tests should preserve its demonstrated contracts using real TypeBox and Pi APIs rather than importing fake interfaces.

## Turn-to-Lavish validation boundary

Detailed renderer validation is deferred to the dedicated turn-to-Lavish rendering-engine tangent. That exploration defines the engine converting Current understanding, stable turn shell, review cards and controls, and unrestricted question context into a whole-document artifact, including submission transport and failure behavior.

Production validation must not restore mandatory Markdown parity for rich-only context or fix screenshot/browsing assertions before the renderer architecture exists. Once the engine is settled, its deterministic seams and browser automation become part of the automated validation surface.

## Sensible interaction efficiency

Token efficiency is design hygiene, not a budget program. There are no hard byte/token caps, canonical tokenizer, or percentage regression gates.

Tools and adapters should apply obvious structural efficiency:

- pass IDs and references instead of repeating stored objects;
- accept narrow domain changes instead of requiring complete state documents;
- batch related mutations when that reduces ceremony without obscuring intent;
- avoid echoing agent-authored prose in mutation receipts;
- return compact outputs shaped to the requested projection;
- keep complete state and generated documents out of routine model-facing results;
- use file or artifact references when genuinely large content already exists externally;
- dynamically activate brainstorm schemas/instructions only in brainstorm mode.

Meaningful context and straightforward tool ergonomics take priority over compression. New encodings, indirection, or file handoffs require evidence of a concrete wasteful hotspot; they are not introduced solely to improve an abstract token count.

## Automated acceptance direction

Production implementation should provide automated evidence for the object-tools domain contract and the Pi-adapter lifecycle before replacing the legacy command. Exact test files and framework are implementation choices, but failures in deterministic state, adapter, mutation, recovery, or transport boundaries block delivery.
