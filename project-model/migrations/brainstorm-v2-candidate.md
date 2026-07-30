# Shared project-model migration candidate

Status: accepted and cut over to authoritative project-model state.

## Cutover record

- Accepted candidate manifest: `sha256:27a871e7e29e58cc800e9e9842a10c26b2660424078648d5124e8650c3119acd`
- Accepted at: `2026-07-29T00:02:20.711Z`
- Receipt mode: `migration_cutover`
- Receipt batch: `sha256:81aacadf72cbb0bc418ae7f885076fb5662e8b3defe4e6412ca4d88051ae1f00`
- Resulting authoritative model hash: `sha256:f88fa748405dc5fc502f08d7f38f7f469ec503d3e3d9984d771f03e8da2fb1f1`
- Governing receipts created: 104
- Generated specs synchronized: 4

## Candidate summary

- Project: Pi DAG Workflow
- Mode: candidate
- Candidate manifest hash: `sha256:27a871e7e29e58cc800e9e9842a10c26b2660424078648d5124e8650c3119acd`
- Mapped objects: 145
- Omitted legacy objects: 568
- Existing specification Markdown inputs: 14
- commitments: 59
- decisions: 38
- discoveries: 19
- evidence: 5
- proposals: 15
- questions: 5
- workstreams: 4

## Required audit

1. Confirm candidate intent/decision/commitment classification.
2. Confirm every omitted active contract is truly superseded or redundant.
3. Compare generated candidate specs with each current functional/supporting spec.
4. Resolve contradictory or missing behavior before creating migration-cutover receipts.
5. Confirm prototype links remain evidence rather than authority.

## Warnings

- Candidate objects intentionally have no human acceptance receipts; cutover acceptance must bind their final semantic hashes.
- Seven intent/concept/scenario objects were semantically derived from accepted legacy decisions and require explicit audit alongside one-to-one mappings.
- Generated candidate specs restructure legacy supporting documents; semantic coverage must be reviewed before those files are retired.

## Existing spec inputs

- `spec/mixed-initiative-project-model/spec.md`
- `spec/prototypes/brainstorm-pi-adapter/README.md`
- `spec/prototypes/object-tools/README.md`
- `spec/prototypes/spec.md`
- `spec/spec.md`
- `spec/structured-brainstorming/object-model.md`
- `spec/structured-brainstorming/pi-integration.md`
- `spec/structured-brainstorming/planning-and-archive.md`
- `spec/structured-brainstorming/production-validation.md`
- `spec/structured-brainstorming/prototypes.md`
- `spec/structured-brainstorming/spec.md`
- `spec/structured-brainstorming/specification-layout.md`
- `spec/structured-brainstorming/tools.md`
- `spec/structured-brainstorming/workflow.md`

## Object mapping

| Legacy ID | Disposition | Candidate target | Reason |
|---|---|---|---|
| `agent-workflow` | mapped | `workstreams/WS-agent-workflow` | Legacy neighborhood became a non-authoritative workstream. |
| `artifacts-lavish` | mapped | `workstreams/WS-artifacts-lavish` | Legacy neighborhood became a non-authoritative workstream. |
| `B-brainstorm-pi-adapter-prototype` | mapped | `discoveries/DISC-brainstorm-pi-adapter-prototype` | Legacy probe retained as a prototype discovery. |
| `D-audited-candidate-atomic-cutover` | mapped | `decisions/DEC-audited-candidate-atomic-cutover` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-authority-aware-deletion` | mapped | `decisions/DEC-authority-aware-deletion` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-awareness-plus-exact-decisions` | mapped | `decisions/DEC-awareness-plus-exact-decisions` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-brainstorm-authority-cutover-disable-downstream` | mapped | `decisions/DEC-brainstorm-authority-cutover-disable-downstream` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-canonical-mixed-initiative-project-model` | mapped | `decisions/DEC-canonical-mixed-initiative-project-model` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-command-native-resume-selection` | omitted | — | Superseded by focus-session commands over one model. |
| `D-command-seed-classified-user-input` | mapped | `decisions/DEC-command-seed-classified-user-input` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-component-vocabulary` | mapped | `discoveries/DISC-deferred-turn-lavish-renderer` | Renderer or planning detail remains a deferred non-governing discovery. |
| `D-conductor-owned-reconciliation` | mapped | `discoveries/DISC-deferred-planning-execution` | Renderer or planning detail remains a deferred non-governing discovery. |
| `D-content-bound-acceptance-receipt` | mapped | `decisions/DEC-content-bound-acceptance-receipt` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-context-block-model` | mapped | `discoveries/DISC-deferred-turn-lavish-renderer` | Renderer or planning detail remains a deferred non-governing discovery. |
| `D-context-interactivity` | mapped | `discoveries/DISC-deferred-turn-lavish-renderer` | Renderer or planning detail remains a deferred non-governing discovery. |
| `D-direct-user-commit-derived-review` | mapped | `decisions/DEC-direct-user-commit-derived-review` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-discoveries-throughout-project-loop` | mapped | `decisions/DEC-discoveries-throughout-project-loop` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-distinct-intent-decision-commitment` | mapped | `decisions/DEC-distinct-intent-decision-commitment` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-durable-statused-snapshot` | mapped | `decisions/DEC-durable-statused-snapshot` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-effective-under-review-visible` | mapped | `decisions/DEC-effective-under-review-visible` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-ephemeral-session-baseline` | mapped | `decisions/DEC-ephemeral-session-baseline` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-evidence-stack` | mapped | `commitments/COM-evidence-stack` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D-explicit-accepted-spec-eligible-types` | mapped | `decisions/DEC-explicit-accepted-spec-eligible-types` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-explicit-brainstorm-runtime-mode` | omitted | — | Superseded by exact linked model focus lifecycle. |
| `D-explicit-focus-session-subcommands` | mapped | `decisions/DEC-explicit-focus-session-subcommands` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-explicit-scope-typed-edges` | mapped | `decisions/DEC-explicit-scope-typed-edges` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-generated-reviewable-views` | omitted | — | Semantic audit classified this legacy contract as obsolete or conflicting. |
| `D-immediate-nonauthoritative-model-write` | mapped | `decisions/DEC-immediate-nonauthoritative-model-write` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-inherit-exact-focus-across-fork` | mapped | `decisions/DEC-inherit-exact-focus-across-fork` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-initial-orientation-proof` | mapped | `commitments/COM-initial-orientation-proof` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D-lean-typed-v1-model` | mapped | `decisions/DEC-lean-typed-v1-model` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-materiality-based-review-turns` | mapped | `decisions/DEC-materiality-based-review-turns` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-meaningful-semantic-refresh` | mapped | `commitments/COM-meaningful-semantic-refresh` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D-minimal-projection-safety-no-manifest` | mapped | `decisions/DEC-minimal-projection-safety-no-manifest` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-mode-system-prompt-plus-kickoff` | mapped | `commitments/COM-mode-system-prompt-plus-kickoff` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D-model-delta-oversight-loop` | mapped | `decisions/DEC-model-delta-oversight-loop` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-model-metadata-projection-declarations` | mapped | `decisions/DEC-model-metadata-projection-declarations` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-model-prose-deterministic-projections` | mapped | `decisions/DEC-model-prose-deterministic-projections` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-multiple-ephemeral-sessions-one-model` | mapped | `decisions/DEC-multiple-ephemeral-sessions-one-model` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-point-level-hash-resolution` | mapped | `decisions/DEC-point-level-hash-resolution` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-port-core-into-existing-extension` | omitted | — | Superseded module and authority boundary. |
| `D-question-context` | mapped | `commitments/COM-question-context` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D-question-corrections` | mapped | `commitments/COM-question-corrections` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D-question-worthiness` | mapped | `commitments/COM-question-worthiness` | Current behavioral commitment retained pending cutover receipt. |
| `D-real-options-only` | mapped | `commitments/COM-real-options-only` | Current behavioral commitment retained pending cutover receipt. |
| `D-reconsideration-question-preserves-effect` | mapped | `decisions/DEC-reconsideration-question-preserves-effect` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-recovery-matrix` | omitted | — | Semantic audit classified this legacy contract as obsolete or conflicting. |
| `D-renderer-parity` | mapped | `discoveries/DISC-deferred-turn-lavish-renderer` | Renderer or planning detail remains a deferred non-governing discovery. |
| `D-renderer-policy` | mapped | `discoveries/DISC-deferred-turn-lavish-renderer` | Renderer or planning detail remains a deferred non-governing discovery. |
| `D-research-classify-before-ask` | mapped | `commitments/COM-research-classify-before-ask` | Current behavioral commitment retained pending cutover receipt. |
| `D-retain-legacy-readonly-diagnostics` | mapped | `decisions/DEC-retain-legacy-readonly-diagnostics` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-retire-grillme` | mapped | `commitments/COM-retire-grillme` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D-review-tool-presentation-adapter` | omitted | — | Lavish adapter is deferred; v1 is chat-only. |
| `D-rich-fragment-isolation` | mapped | `discoveries/DISC-deferred-turn-lavish-renderer` | Renderer or planning detail remains a deferred non-governing discovery. |
| `D-semantic-current-state-import` | mapped | `decisions/DEC-semantic-current-state-import` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-semantic-evaluation` | mapped | `commitments/COM-semantic-evaluation` | Current behavioral commitment retained pending cutover receipt. |
| `D-separate-tension-type` | mapped | `decisions/DEC-separate-tension-type` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-single-canonical-placement-links` | mapped | `decisions/DEC-single-canonical-placement-links` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-single-current-proof` | mapped | `commitments/COM-single-current-proof` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D-single-cutover-no-dual-authority` | omitted | — | Semantic audit classified this legacy contract as obsolete or conflicting. |
| `D-single-repository-model` | mapped | `decisions/DEC-single-repository-model` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-six-explicit-model-tools` | mapped | `decisions/DEC-six-explicit-model-tools` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-structural-proof-validation` | mapped | `commitments/COM-structural-proof-validation` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D-token-budget` | mapped | `commitments/COM-token-budget` | Current behavioral commitment retained pending cutover receipt. |
| `D-tracked-nonauthoritative-synthesis` | mapped | `decisions/DEC-tracked-nonauthoritative-synthesis` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-tracked-project-model-json` | mapped | `decisions/DEC-tracked-project-model-json` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-tracked-specs-ephemeral-oversight` | mapped | `decisions/DEC-tracked-specs-ephemeral-oversight` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-transactional-automatic-spec-sync` | mapped | `decisions/DEC-transactional-automatic-spec-sync` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-type-state-separate-authority` | mapped | `decisions/DEC-type-state-separate-authority` | Shared-model-era accepted choice is a candidate decision pending cutover receipt. |
| `D-understanding-in-resolve-review` | mapped | `commitments/COM-understanding-in-resolve-review` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D1-contextual-working-sets` | mapped | `commitments/COM-contextual-working-sets` | Current behavioral commitment retained pending cutover receipt. |
| `D1-no-resurrection` | mapped | `commitments/COM-no-resurrection` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D1-question-distinction` | mapped | `commitments/COM-question-distinction` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D20-openspec-conventions-only` | mapped | `commitments/COM-openspec-conventions-only` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D21-retain-structured-brainstorm` | omitted | — | Superseded by mixed-initiative model brainstorming. |
| `D22-no-openspec-changes` | mapped | `commitments/COM-no-openspec-changes` | Current behavioral commitment retained pending cutover receipt. |
| `D23-dag-native-planning` | omitted | — | Planning is explicitly deferred and disabled. |
| `D24-keep-spec-root` | mapped | `commitments/COM-keep-spec-root` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D25-structured-discovery-loop` | mapped | `commitments/COM-structured-discovery-loop` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D26-bounded-autonomous-bursts` | mapped | `commitments/COM-bounded-autonomous-bursts` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D27-user-selects-tangents` | mapped | `commitments/COM-user-selects-tangents` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D28-optional-reasoned-recommendations` | mapped | `commitments/COM-optional-reasoned-recommendations` | Current behavioral commitment retained pending cutover receipt. |
| `D29-user-authorizes-planning` | omitted | — | Planning is explicitly deferred and disabled. |
| `D30-fixed-tangent-badges` | mapped | `discoveries/DISC-deferred-turn-lavish-renderer` | Renderer or planning detail remains a deferred non-governing discovery. |
| `D31-agent-owned-ignorable-badges` | mapped | `discoveries/DISC-deferred-turn-lavish-renderer` | Renderer or planning detail remains a deferred non-governing discovery. |
| `D32-flat-nonclosed-frontier` | mapped | `discoveries/DISC-deferred-turn-lavish-renderer` | Renderer or planning detail remains a deferred non-governing discovery. |
| `D33-unrestricted-tangent-multiselect` | mapped | `commitments/COM-unrestricted-tangent-multiselect` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D34-no-tangent-selection-lease` | mapped | `commitments/COM-no-tangent-selection-lease` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D37-integrate-clear-preserve-unclear` | mapped | `commitments/COM-integrate-clear-preserve-unclear` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D39-user-requests-prototypes` | mapped | `commitments/COM-user-requests-prototypes` | Current behavioral commitment retained pending cutover receipt. |
| `D4-archive-history` | omitted | — | Archive/history behavior is outside brainstorming-first v1. |
| `D40-prototypes-start-under-spec` | mapped | `commitments/COM-prototypes-start-under-spec` | Current behavioral commitment retained pending cutover receipt. |
| `D41-prototype-readme` | mapped | `commitments/COM-prototype-readme` | Current behavioral commitment retained pending cutover receipt. |
| `D42-curate-prototypes` | mapped | `commitments/COM-curate-prototypes` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D44-prototype-bounded-behavioral-reference` | mapped | `commitments/COM-prototype-bounded-behavioral-reference` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D45-immediate-spec-prototype-ambiguity` | mapped | `commitments/COM-immediate-spec-prototype-ambiguity` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D46-plan-and-chunk-assign-prototypes` | mapped | `discoveries/DISC-deferred-planning-execution` | Renderer or planning detail remains a deferred non-governing discovery. |
| `D47-prototype-readme-execution-and-scope` | mapped | `commitments/COM-prototype-readme-execution-and-scope` | Current behavioral commitment retained pending cutover receipt. |
| `D48-retain-useful-prototypes` | mapped | `commitments/COM-retain-useful-prototypes` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D49-relevant-first-spec-traversal` | mapped | `discoveries/DISC-deferred-planning-execution` | Renderer or planning detail remains a deferred non-governing discovery. |
| `D51-flexible-requirements-and-scenarios` | mapped | `commitments/COM-flexible-requirements-and-scenarios` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D52-short-linked-support-summaries` | mapped | `commitments/COM-short-linked-support-summaries` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D53-prototype-specmd-exception` | mapped | `commitments/COM-prototype-specmd-exception` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D54-always-ask-resume-or-new` | omitted | — | Superseded by one model with multiple focus sessions. |
| `D55-compact-resume-orientation` | mapped | `commitments/COM-compact-resume-orientation` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D56-json-projectmd-boundary` | omitted | — | Superseded by tracked model authority and generated specs. |
| `D57-refresh-selected-focus-on-resume` | mapped | `commitments/COM-refresh-selected-focus-on-resume` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D58-reconstruct-missing-state` | omitted | — | Superseded by durable model plus disposable focus recovery. |
| `D59-continuous-tangent-discovery` | mapped | `commitments/COM-continuous-tangent-discovery` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D6-derived-views` | mapped | `commitments/COM-derived-views` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D6-lifecycle-authority` | omitted | — | Superseded by durable shared-model authority. |
| `D6-single-agent` | mapped | `commitments/COM-single-agent` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D6-snapshot-only` | mapped | `commitments/COM-snapshot-only` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D6-specs-survive` | omitted | — | Superseded by deterministic model-owned generated specs. |
| `D6-worktree-isolation` | omitted | — | Semantic audit classified this legacy contract as obsolete or conflicting. |
| `D60-show-tangents-at-review-checkpoint` | mapped | `commitments/COM-show-tangents-at-review-checkpoint` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D61-tangent-candidate-context` | mapped | `commitments/COM-tangent-candidate-context` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D62-untouched-tangents-become-tracked` | omitted | — | Semantic audit classified this legacy contract as obsolete or conflicting. |
| `D63-out-of-scope-is-brainstorm-local` | omitted | — | Semantic audit classified this legacy contract as obsolete or conflicting. |
| `D64-agent-semantically-deduplicates-tangents` | mapped | `commitments/COM-agent-semantically-deduplicates-tangents` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D65-no-forced-exhaustion-sweep` | mapped | `commitments/COM-no-forced-exhaustion-sweep` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D66-agent-judges-tangents-tools-validate-structure` | mapped | `commitments/COM-agent-judges-tangents-tools-validate-structure` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D67-defer-toon` | mapped | `commitments/COM-defer-toon` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D68-renderer-neutral-direct-integration` | mapped | `discoveries/DISC-deferred-turn-lavish-renderer` | Renderer or planning detail remains a deferred non-governing discovery. |
| `D69-judgment-structure-direction-transport` | mapped | `commitments/COM-judgment-structure-direction-transport` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D7-global-history` | omitted | — | History generation is not part of the current-snapshot projector. |
| `D7-inline-rationale` | mapped | `commitments/COM-inline-rationale` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D7-markdown-links` | mapped | `commitments/COM-markdown-links` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D7-markdown-only` | mapped | `commitments/COM-markdown-only` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D7-primary-spec` | omitted | — | Semantic audit classified this legacy contract as obsolete or conflicting. |
| `D7-prototypes-directory` | mapped | `commitments/COM-prototypes-directory` | Current behavioral commitment retained pending cutover receipt. |
| `D70-four-layer-tool-output` | mapped | `commitments/COM-four-layer-tool-output` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D71-context-before-review-decisions` | mapped | `commitments/COM-context-before-review-decisions` | Current behavioral commitment retained pending cutover receipt. |
| `D72-review-may-suggest-optional-lavish` | mapped | `discoveries/DISC-deferred-turn-lavish-renderer` | Renderer or planning detail remains a deferred non-governing discovery. |
| `D73-resolve-review-before-promotion` | omitted | — | Promotion is removed. |
| `D74-mechanical-inference-only` | mapped | `commitments/COM-mechanical-inference-only` | Legacy contract was semantically rewritten into model-native candidate commitment prose. |
| `D75-five-production-brainstorm-tools` | omitted | — | Superseded by six project-model tools. |
| `E-accepted-five-tool-adapter` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-accepted-renderer-boundaries` | mapped | `evidence/EV-accepted-renderer-boundaries` | Evidence is referenced by an unresolved migrated question. |
| `E-adapter-prototype-findings` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-brainstorm-first-authority-user-selection` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-browser-isolation-options` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-current-authority-conflict-audit` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-current-context-size-baseline` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-current-cutover-readiness-audit` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-current-understanding-user-resolution` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-current-validation-surface` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-cutover-readiness-user-decisions` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-deterministic-projection-contract-audit` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-existing-dag-extension-command` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-existing-renderer-contract` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-existing-turn-artifact-findings` | mapped | `evidence/EV-existing-turn-artifact-findings` | Evidence is referenced by an unresolved migrated question. |
| `E-html-sanitizer-ecosystem` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-lavish-artifact-boundary` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-lavish-cli-lifecycle` | mapped | `evidence/EV-lavish-cli-lifecycle` | Evidence is referenced by an unresolved migrated question. |
| `E-lavish-feedback-shape` | mapped | `evidence/EV-lavish-feedback-shape` | Evidence is referenced by an unresolved migrated question. |
| `E-lavish-integration-protocol-gap` | mapped | `evidence/EV-lavish-integration-protocol-gap` | Evidence is referenced by an unresolved migrated question. |
| `E-lavish-review-handoff` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-mixed-initiative-project-model-overview` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-mixed-initiative-research-foundations` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-mixed-initiative-turn-loop-audit` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-model-tools-pi-integration-audit` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-model-tools-user-resolution` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-on-the-loop-research` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-pi-command-ui-modes` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-pi-dynamic-tool-lifecycle` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-pi-integration-user-resolution` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-pi-renderer-capabilities` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-pi-tool-rendering-and-state` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-production-test-seams` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-projection-frontier-context-correction` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-projection-product-decisions` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-question-quality-dogfood` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-semantic-quality-contracts` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-shared-mental-model-research` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-shared-model-adoption` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-shared-model-compatibility-audit` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-t19-user-guidance` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-t8-document-shell-clarification` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-t8-user-flexibility-guidance` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-turn-loop-final-user-resolution` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-turn-loop-partial-user-resolution` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-under-review-projection-user-selection` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-understanding-proof-state-boundary` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-understanding-proof-user-intent` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-v1-schema-requirements-audit` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-v1-schema-user-resolution` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-validation-classification-policy` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-validation-user-automation-boundary` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `E-validation-user-prototype-renderer-efficiency` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-context-compaction` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-current-archive-prompt` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-current-spec-prototype` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-current-tool-audit` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-divergent-frontier-renewal` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-double-diamond` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-feedback-prototype-gap` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-frontier-prototype` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-kiro-topology` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-object-prototype` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-object-tools-prototype-lifecycle` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-openspec-custom` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-openspec-explore` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-openspec-overview` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-openspec-package` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-openspec-pi` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-openspec-topology` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-operant-sessions` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-operant-sparse-feedback` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-opportunity-tree` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-pi-tool-output-boundary` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-project-understanding` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-prototype-location-direction` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-snapshot-resume` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-spec-kit-layout` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-speckit-topology` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-tc01-question-quality` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-tool-dogfood-metrics` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-toon-benchmark` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-user-brainstorm-value` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-user-prototype-authority` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-user-tangent-control` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `ev-user-tangent-expansion` | omitted | — | Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful. |
| `lifecycle-authority` | mapped | `workstreams/WS-lifecycle-authority` | Legacy neighborhood became a non-authoritative workstream. |
| `M-brainstorming-first-cutover-contract` | omitted | — | Promotion is obsolete workflow history; target specs are migration inputs. |
| `M-correction-conflict-refinement` | omitted | — | Promotion is obsolete workflow history; target specs are migration inputs. |
| `M-current-understanding-proof-lifecycle` | omitted | — | Promotion is obsolete workflow history; target specs are migration inputs. |
| `M-deterministic-projection-contract` | omitted | — | Promotion is obsolete workflow history; target specs are migration inputs. |
| `M-freeform-context-rendering-boundary` | omitted | — | Promotion is obsolete workflow history; target specs are migration inputs. |
| `M-historical-contract-audit` | omitted | — | Promotion is obsolete workflow history; target specs are migration inputs. |
| `M-mixed-initiative-project-model-adoption` | omitted | — | Promotion is obsolete workflow history; target specs are migration inputs. |
| `M-mixed-initiative-turn-loop` | omitted | — | Promotion is obsolete workflow history; target specs are migration inputs. |
| `M-pi-extension-integration` | omitted | — | Promotion is obsolete workflow history; target specs are migration inputs. |
| `M-probing-question-quality` | omitted | — | Promotion is obsolete workflow history; target specs are migration inputs. |
| `M-production-model-tools-command-lifecycle` | omitted | — | Promotion is obsolete workflow history; target specs are migration inputs. |
| `M-production-validation` | omitted | — | Promotion is obsolete workflow history; target specs are migration inputs. |
| `M-project-model-v1-semantic-core` | omitted | — | Promotion is obsolete workflow history; target specs are migration inputs. |
| `P-accept-against-current-content` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-accept-correction-reframe` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-accepted-only-durable-model` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-accepted-only-import` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-agent-authored-whole-document` | mapped | `proposals/PROP-agent-authored-whole-document` | Candidate option remains attached to an unresolved question. |
| `P-agent-auto-commit-implications` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-agent-context-controls` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-agent-driven-resume-selection` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-agent-managed-lavish-handoff` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-agent-regenerated-prose` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-all-input-needs-review` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-all-validation-blocking` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-always-active-brainstorm-tools` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-always-manual-reactivation` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-always-overwrite-declared-paths` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-always-structured-options` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-approve-whole-model-delta` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-artifact-approval-loop` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-ask-early-to-codiscover` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-async-renderer-message-handoff` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-audited-candidate-atomic-cutover` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-authority-aware-deletion` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-auto-accept-derived-implications` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-awareness-plus-exact-decisions` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-blocking-adapter-recovery-matrix` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-bounded-autonomous-run` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-bounded-intent-transport` | mapped | `proposals/PROP-bounded-intent-transport` | Candidate option remains attached to an unresolved question. |
| `P-brainstorm-authority-cutover-disable-downstream` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-brainstorm-cutover-legacy-downstream-compatibility` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-brainstorm-first-cutover` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-bundled-client-app-shell` | mapped | `proposals/PROP-bundled-client-app-shell` | Candidate option remains attached to an unresolved question. |
| `P-candidate-only-brainstorm-dogfood` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-canonical-mixed-initiative-project-model` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-cdn-daisy-shell` | mapped | `proposals/PROP-cdn-daisy-shell` | Candidate option remains attached to an unresolved question. |
| `P-checkpoint-every-user-turn` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-ci-only-spec-synchronization` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-clone-focus-on-fork` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-command-native-resume-selection` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-command-seed-always-authoritative` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-command-seed-classified-user-input` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-command-seed-never-authoritative` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-compact-context-ingredients` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-conductor-owned-reconciliation` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-confidence-threshold` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-confirm-correction` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-container-owned-objects` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-content-bound-acceptance-receipt` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-cross-renderer-equivalence-tests` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-decision-only-refresh` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-decisions-only-authority` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-derive-paths-from-workstreams` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-derived-rich-fallback` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-deterministic-only-validation` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-deterministic-understanding-template` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-direct-in-place-cutover` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-direct-user-commit-derived-review` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-distinct-intent-decision-commitment` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-domain-scenario-only-validation` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-dual-model-spec-authority` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-duplicate-exact-bodies-across-views` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-durable-statused-snapshot` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-editable-generated-specs` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-effective-under-review-visible` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-ephemeral-session-baseline` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-ephemeral-session-synthesis` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-event-sourced-project-model` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-every-user-turn-refresh` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-exclude-any-challenged-content` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-explicit-accepted-spec-eligible-types` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-explicit-brainstorm-runtime-mode` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-explicit-focus-session-subcommands` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-explicit-scope-typed-edges` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-explicit-spec-generation-tool` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-extended-shadow-mode` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-finish-then-reconcile-all` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-five-tools-update-can-authorize` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-fixed-context-template` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-freeform-first` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-freeform-relations-and-tags` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-full-legacy-import` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-full-project-ontology-v1` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-full-vertical-cutover` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-future-projects-only-model` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-general-component-tree` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-generated-reviewable-views` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-generic-node-edge-model` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-git-only-delta-baseline` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-global-or-npx-lavish` | mapped | `proposals/PROP-global-or-npx-lavish` | Candidate option remains attached to an unresolved question. |
| `P-hard-delete-any-current-object` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-hard-gated-llm-judge` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-immediate-nonauthoritative-model-write` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-include-all-lifecycle-states` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-incremental-dual-write-migration` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-infer-primary-placement` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-initial-orientation-proof` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-instruction-only-proof` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-intent-decision-commitment-only` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-intent-plus-decisions-no-commitments` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-keep-disabled-prompts-and-run-tools` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-kickoff-only-brainstorm-instructions` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-latest-by-default-resume` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-layered-validation-gates` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-lean-typed-v1-model` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-live-pi-only-integration-validation` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-llm-proof-evaluator` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-local-explanation-only` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-manual-dogfood-only-semantic-evals` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-manual-regions-and-reverse-sync` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-markdown-only-context` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-material-user-judgment-threshold` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-materiality-based-checkpoints` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-meaningful-semantic-refresh` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-minimal-markdown-custom-vocabulary` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-modality-specific-renderer-contracts` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-mode-system-prompt-plus-kickoff` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-model-delta-oversight-loop` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-model-last-reviewed-metadata` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-model-metadata-projection-declarations` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-model-per-initiative` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-model-prose-deterministic-projections` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-model-supports-canonical-specs` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-multiple-ephemeral-sessions-one-model` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-nested-sandbox-fragment` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-never-hard-delete-model-objects` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-no-persisted-session` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-omit-initial-proof` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-one-decision-per-turn` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-opaque-arbitrary-markdown-documents` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-optional-cli-protocol-adapter` | mapped | `proposals/PROP-optional-cli-protocol-adapter` | Candidate option remains attached to an unresolved question. |
| `P-pause-on-any-divergence` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-per-review-ephemeral-artifact` | mapped | `proposals/PROP-per-review-ephemeral-artifact` | Candidate option remains attached to an unresolved question. |
| `P-persist-only-accepted-content` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-pi-sessions-are-focus-sessions` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-point-level-hash-resolution` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-port-core-into-existing-extension` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-portable-heading-free-markdown-fragments` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-preserve-existing-docs-as-static-views` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-private-lavish-http-api` | mapped | `proposals/PROP-private-lavish-http-api` | Candidate option remains attached to an unresolved question. |
| `P-projection-and-regression-budgets` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-proof-history` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-prose-only-execution-discoveries` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-prototype-only-migration` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-raw-lavish-poll-output` | mapped | `proposals/PROP-raw-lavish-poll-output` | Candidate option remains attached to an unresolved question. |
| `P-raw-rich-context` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-real-options-only` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-reconsideration-question-preserves-effect` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-reject-whole-stale-packet` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-relationship-inherited-spec-eligibility` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-remove-all-legacy-dag-capabilities` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-renderer-full-state-read` | mapped | `proposals/PROP-renderer-full-state-read` | Candidate option remains attached to an unresolved question. |
| `P-renderer-resolves-submission` | mapped | `proposals/PROP-renderer-resolves-submission` | Candidate option remains attached to an unresolved question. |
| `P-reopened-means-nongoverning` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-required-semantic-fallback` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-research-classify-before-ask` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-research-cost-boundary` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-restore-exact-suspend-new-fork` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-retain-legacy-readonly-diagnostics` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-retain-user-quote-proof` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-retained-lavish-review-artifacts` | mapped | `proposals/PROP-retained-lavish-review-artifacts` | Candidate option remains attached to an unresolved question. |
| `P-review-attached-proofs` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-review-tool-presentation-adapter` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-revise-same-point` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-rich-decision-context` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-rubric-corpus-semantic-evals` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-sanitized-scoped-light-dom` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-self-contained-package-shell` | mapped | `proposals/PROP-self-contained-package-shell` | Candidate option remains attached to an unresolved question. |
| `P-semantic-current-state-import` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-separate-brainstorm-extension` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-separate-effect-and-review-state` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-separate-projection-config` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-separate-tension-type` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-session-sticky-brainstorm-tools` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-shadow-dom-fragment` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-shared-promoted-core` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-single-canonical-placement-links` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-single-current-brainstorm-artifact` | mapped | `proposals/PROP-single-current-brainstorm-artifact` | Candidate option remains attached to an unresolved question. |
| `P-single-current-focus-command` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-single-current-proof` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-single-cutover-no-dual-authority` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-single-ephemeral-session-per-repository` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-single-generic-model-apply` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-single-repository-model` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-single-tokenizer-hard-budgets` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-six-explicit-model-tools` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-small-context-component-vocabulary` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-split-model-collections` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-stage-findings-until-checkpoint` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-static-custom-context` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-static-session-brief` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-status-and-source-only` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-strict-generated-ownership` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-structural-proof-validation` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-surface-all-relevant-uncertainty` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-telemetry-only-token-policy` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-tension-as-question-kind` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-tool-guideline-brainstorm-instructions` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-track-all-standard-views` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-track-no-generated-views` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-tracked-ai-model` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-tracked-nonauthoritative-synthesis` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-tracked-project-model-json` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-tracked-specs-ephemeral-oversight` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-transactional-automatic-spec-sync` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-turn-projection-flexible-context` | mapped | `proposals/PROP-turn-projection-flexible-context` | Candidate option remains attached to an unresolved question. |
| `P-type-state-separate-authority` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-typed-context-blocks` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-typed-rich-block-ast` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-understanding-at-next-review` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-understanding-in-resolve-review` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-understanding-separate-update` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-universal-lifecycle-plus-authority` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-universal-status-lattice` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-unstructured-context` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-untracked-on-demand-views` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-user-requested-checkpoints-only` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-visual-snapshot-renderer-tests` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P-worker-direct-model-mutation` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P10-bounded-bursts` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P10-opinionated-convergence` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P10-ranked-frontier` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P10-scope-readiness` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P10-structured-loop` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P11-auto-single-choose-multiple` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P11-compact-resume-orientation` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P11-focused-reorientation` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P11-graceful-reconstruction` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P11-json-active-project-summary` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P15-change-handoff` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P15-compatible-only` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P15-custom-schema` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P15-full-front-half` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P16-bounded-exhaustion-report` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P16-checkpoint-candidate-batch` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P16-concise-candidate-context` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P16-continuous-expansion-scan` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P16-instruction-judgment-structural-tools` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P16-instruction-scan-tool-support` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P16-semantic-duplicate-check` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P16-session-scope-exclusion` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P16-untouched-stays-proposed` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P17-explicit-mechanical-rules` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P17-four-layer-output` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P17-mechanical-inference` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P17-narrow-delta-artifact-output` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P17-reference-first-objects` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P17-toon-selective-benchmark` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P18-direct-integration` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P18-five-tool-core` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P18-five-tool-flow-detailed` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P18-five-tools-renamed-flow` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P18-immediate-agent-promotion` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P18-judgment-structure-ownership` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P18-resolve-separate-promote` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P18-resolver-writes-specs` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P2-explicit-untouched` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P2-flexible-target-normalization` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P2-local-batch` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P2-message-is-batch` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P2-partial-clear-integration` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P3-explainable-badges` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P3-explicit-cluster-selection` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P3-fixed-semantic-badges` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P3-grouped-full-frontier` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P3-selection-lease` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P5-agents-run-prototypes` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P5-conflict-needs-user-resolution` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P5-evidence-not-authority` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P5-explicit-authority-boundary` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P5-mutable-curation` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P5-readme-local-doc` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P5-retain-until-redundant` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P5-runnable-entrypoint` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P5-spec-from-start` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P5-uncertainty-trigger` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P6-branch-state` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P6-integrated-turns` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P6-layered-authority` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P6-migrations` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P6-snapshot-events` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P6-stale-rebase` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P6-tombstones` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P7-artifacts-folder` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P7-evolve-in-place` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P7-explicit-hierarchy-links` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P7-feature-history` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P7-flexible-requirement-scenarios` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P7-functional-hierarchy` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P7-functional-only-specmd` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P7-initiative-workspace` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P7-linked-support-details` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P7-minimal-spec-link-to-readme` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P7-prototype-exception` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P7-prototype-folders` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P7-prototype-spec-replaces-readme` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P7-relative-links` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P7-relevant-first-traversal` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P7-spec-primary` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `P7-support-spec-index` | omitted | — | Rejected, superseded, accepted-into-decision, or otherwise non-current option. |
| `Q-before-asking` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-brainstorm-dogfood-authority-boundary` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-brainstorm-instruction-delivery` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-brainstorm-mode-lifetime` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-brainstorm-start-resume-ux` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-command-focus-session-surface` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-command-restore-fork-behavior` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-command-seed-authority-eligibility` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-context-block-model` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-context-interactivity-boundary` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-context-shape` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-correction-response` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-cutover-import-policy` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-cutover-legacy-readonly-diagnostics` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-cutover-projection-generation` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-cutover-repository-model-scope` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-cutover-run-autonomy-envelope` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-cutover-storage-shape` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-cutover-user-commit-semantics` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-cutover-v1-schema-depth` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-cutover-verification-rollback` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-cutover-vertical-scope` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-execution-discovery-reconciliation` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-generated-view-authority` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-initial-context-components` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-initial-understanding` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-integration-recovery-fixtures` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-lavish-runtime-protocol` | mapped | `questions/Q-lavish-runtime-protocol` | Open legacy question remains deferred unresolved direction. |
| `Q-model-delta-commit-loop` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-option-quality` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-production-module-boundary` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-projection-canonical-placement` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-projection-drift-manual-edits` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-projection-markdown-fragments` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-projection-routing-declarations` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-projection-spec-eligibility` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-projection-under-review-content` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-projection-view-inventory` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-question-threshold` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-render-artifact-session-lifecycle` | mapped | `questions/Q-render-artifact-session-lifecycle` | Open legacy question remains deferred unresolved direction. |
| `Q-renderer-input-contract` | mapped | `questions/Q-renderer-input-contract` | Open legacy question remains deferred unresolved direction. |
| `Q-renderer-parity-fallback` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-renderer-shell-assets` | mapped | `questions/Q-renderer-shell-assets` | Open legacy question remains deferred unresolved direction. |
| `Q-renderer-submission-handoff` | mapped | `questions/Q-renderer-submission-handoff` | Open legacy question remains deferred unresolved direction. |
| `Q-renderer-validation-policy` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-review-renderer-handoff` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-rich-fragment-isolation` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-semantic-behavior-evaluation` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-shared-model-authority` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-shared-model-cutover` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-shared-model-durability` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-token-efficiency-budget` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-tools-six-operation-boundary` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-tools-spec-synchronization` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-turn-checkpoint-trigger` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-turn-current-understanding-persistence` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-turn-delta-baseline` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-turn-exploration-mutation-timing` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-turn-review-acceptance-surface` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-turn-session-focus-model` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-turn-stale-sparse-resolution` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-understanding-generation-flow` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-understanding-refresh-trigger` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-understanding-retention` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-understanding-validation` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-v1-acceptance-receipt` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-v1-deletion-supersession` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-v1-intent-decision-commitment` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-v1-reconsideration-effect` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-v1-scope-relationships` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-v1-state-authority-axes` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-v1-tension-type` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q-validation-evidence-stack` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q10-autonomy-cadence` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q10-frontier-ranking` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q10-mode-loop` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q10-readiness` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q10-recommendation-stance` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q11-missing-state` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q11-projectmd-boundary` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q11-repository-refresh` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q11-resume-selection` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q11-resume-surface` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q15-changes` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q15-fit` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q15-model` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q15-path-archive` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q15-plan-handoff` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q16-candidate-context` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q16-discovery-duty` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q16-duplicate-suppression` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q16-exhaustion-evidence` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q16-implicit-disposition` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q16-out-of-scope-duration` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q16-proposal-timing` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q16-scan-mechanism` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q17-assumption-level` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q17-read-render-output` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q17-toon-role` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q18-axi-tool-flow` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q18-feedback-integration-tool` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q18-judgment-enforcement-matrix` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q18-production-tool-surface` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q18-review-resolution-promotion` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q2-ambiguous-feedback` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q2-feedback-targeting` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q2-integration-trigger` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q2-submission-model` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q2-untouched-semantics` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q3-activation-duration` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q3-badge-authority` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q3-badge-vocabulary` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q3-frontier-visibility` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q3-selection-cardinality` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q4-archive-promotion-gate` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q5-authoritative-scope` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q5-authority` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q5-creation-trigger` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q5-executable-contract` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q5-initial-location` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q5-local-document` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q5-planning-use` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q5-post-implementation` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q5-retention` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q5-spec-prototype-conflict` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q6-branch-identity` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q6-concurrency` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q6-deletion` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q6-derived-views` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q6-durability-boundary` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q6-migrations` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q6-ownership` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q6-snapshot-events` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q6-turn-history` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q7-content-format` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q7-evolution-discovery` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q7-history` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q7-links` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q7-primary-doc` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q7-prototype-readme-specmd-overlap` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q7-prototypes` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q7-rationale` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q7-rendered-artifacts` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q7-support-index` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q7-supporting-doc-authority` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `Q7-workspace-unit` | omitted | — | Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions. |
| `surface-interaction` | mapped | `workstreams/WS-surface-interaction` | Legacy neighborhood became a non-authoritative workstream. |
| `T-agent-prompt-evaluation` | mapped | `discoveries/DISC-agent-prompt-evaluation` | Non-closed tangent remains a deferred discovery/frontier item. |
| `T-current-understanding-proof` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T-grillme-overlap` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T-mixed-initiative-project-model` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T-mixed-initiative-turn-loop` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T-model-execution-reconciliation` | mapped | `discoveries/DISC-deferred-planning-execution` | Deferred tangent consolidated into the matching non-governing discovery. |
| `T-model-projections-divergence` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T-pi-extension-integration` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T-production-validation` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T-project-model-storage-versioning` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T-project-model-v1-schema` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T-shared-model-cutover-tools` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T-turn-lavish-renderer` | mapped | `discoveries/DISC-deferred-turn-lavish-renderer` | Deferred tangent consolidated into the matching non-governing discovery. |
| `T1-surface-anatomy` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T10-exploration` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T11-context-continuity` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T13-planning-handoff` | mapped | `discoveries/DISC-deferred-planning-execution` | Deferred tangent consolidated into the matching non-governing discovery. |
| `T14-concurrency` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T15-openspec-adoption` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T16-tangent-creation` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T17-token-efficient-updates` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T18-instruction-tool-boundary` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T19-probing-question-quality` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T2-feedback-batching` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T3-tangent-map` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T4-promotion` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T5-prototypes` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T6-state-authority` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T7-spec-topology` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T8-rich-rendering` | omitted | — | Closed or out-of-scope legacy exploration branch. |
| `T9-multidoc-lavish` | omitted | — | Closed or out-of-scope legacy exploration branch. |
