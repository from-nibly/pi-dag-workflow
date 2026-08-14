import { readFile, readdir } from "node:fs/promises";
import { join, resolve } from "node:path";
import { allocateObjectId, candidateManifestHash, semanticHash, slugify } from "./model.ts";
import type { ModelCollectionName, ModelObject, ProjectModel, SpecProjectionView } from "./types.ts";
import { createEmptyModel } from "./model.ts";

interface LegacyEntity { id: string; title?: string; key?: string; status?: string; createdAt?: string; updatedAt?: string; [key: string]: any }
interface LegacyState {
  id: string;
  title: string;
  neighborhoods: LegacyEntity[];
  tangents: LegacyEntity[];
  questions: LegacyEntity[];
  evidence: LegacyEntity[];
  proposals: LegacyEntity[];
  probes: LegacyEntity[];
  decisions: LegacyEntity[];
  promotions: LegacyEntity[];
  currentUnderstanding?: any;
}

export interface MigrationMapping {
  legacyId: string;
  targetId?: string;
  targetCollection?: ModelCollectionName;
  disposition: "mapped" | "omitted";
  reason: string;
}

export interface MigrationResult {
  model: ProjectModel;
  mappings: MigrationMapping[];
  warnings: string[];
  report: string;
}

const SUPERSEDED_LEGACY_DECISIONS = new Map<string, string>([
  ["D6-lifecycle-authority", "Superseded by durable shared-model authority."],
  ["D6-specs-survive", "Superseded by deterministic model-owned generated specs."],
  ["D4-archive-history", "Archive/history behavior is outside brainstorming-first v1."],
  ["D7-global-history", "History generation is not part of the current-snapshot projector."],
  ["D21-retain-structured-brainstorm", "Superseded by mixed-initiative model brainstorming."],
  ["D23-dag-native-planning", "Legacy planning authority is not migrated; current model-aware planning is governed by post-migration model decisions."],
  ["D29-user-authorizes-planning", "Legacy planning authorization is not migrated; current approval and authorization are governed by post-migration model decisions."],
  ["D54-always-ask-resume-or-new", "Superseded by one model with multiple focus sessions."],
  ["D56-json-projectmd-boundary", "Superseded by tracked model authority and generated specs."],
  ["D58-reconstruct-missing-state", "Superseded by durable model plus disposable focus recovery."],
  ["D73-resolve-review-before-promotion", "Promotion is removed."],
  ["D75-five-production-brainstorm-tools", "Superseded by six project-model tools."],
  ["D-port-core-into-existing-extension", "Superseded module and authority boundary."],
  ["D-command-native-resume-selection", "Superseded by focus-session commands over one model."],
  ["D-explicit-brainstorm-runtime-mode", "Superseded by exact linked model focus lifecycle."],
  ["D-review-tool-presentation-adapter", "Lavish adapter is deferred; v1 is chat-only."],
]);

const NEW_MODEL_DECISION_START = "D-canonical-mixed-initiative-project-model";

export async function migrateLegacyBrainstorm(root: string, legacy: LegacyState): Promise<MigrationResult> {
  const overrides = await loadMigrationOverrides(root);
  const model = createEmptyModel(legacy.id || "project", legacy.title || "Project", "candidate");
  model.project.id = slugify(resolve(root).split("/").pop() || legacy.id || "project");
  model.project.title = "Pi DAG Workflow";
  const mappings: MigrationMapping[] = [];
  const warnings: string[] = [];
  const legacyToNew = new Map<string, string>();

  for (const neighborhood of legacy.neighborhoods ?? []) {
    const object = addObject(model, "workstreams", neighborhood.id, {
      title: neighborhood.title ?? neighborhood.id,
      body: `Project-model workstream migrated from legacy design neighborhood ${neighborhood.id}.`,
      state: neighborhood.status === "active" ? "active" : "deferred",
      scope: { kind: "repository" },
      introducedBy: "migration",
      sourceRefs: [`legacy-brainstorm:${neighborhood.id}`],
      relationships: [],
    });
    legacyToNew.set(neighborhood.id, object.id);
    mappings.push(mapped(neighborhood.id, "workstreams", object.id, "Legacy neighborhood became a non-authoritative workstream."));
  }

  const derivedIntentIds = [
    addObject(model, "intents", "scalable-oversight", {
      title: "Scale human oversight through a shared project model",
      body: "Concentrate human attention on desired outcomes, priorities, material model changes, misunderstandings, consequences, and representative scenarios while the agent owns research and model-maintenance labor.",
      kind: "outcome",
      state: "proposed",
      scope: { kind: "repository" }, introducedBy: "migration", sourceRefs: ["legacy-brainstorm:D-canonical-mixed-initiative-project-model"], relationships: [],
    }).id,
    addObject(model, "intents", "human-authority", {
      title: "Preserve explicit human authority over project direction",
      body: "Humans own outcomes, priorities, values, non-goals, and acceptance or replacement of governing direction; silence and agent-derived implications never commit.",
      kind: "value",
      state: "proposed",
      scope: { kind: "repository" }, introducedBy: "migration", sourceRefs: ["legacy-brainstorm:D-model-delta-oversight-loop"], relationships: [],
    }).id,
    addObject(model, "intents", "brainstorm-first-dogfood", {
      title: "Dogfood model brainstorming before redesigning execution",
      body: "Switch and evaluate the complete brainstorming slice first, with model-unaware planning and execution disabled until field experience informs their model-aware replacements.",
      kind: "priority",
      state: "proposed",
      scope: { kind: "repository" }, introducedBy: "migration", sourceRefs: ["legacy-brainstorm:D-brainstorm-authority-cutover-disable-downstream"], relationships: [],
    }).id,
  ];
  const derivedConceptIds = [
    addObject(model, "concepts", "focus-session", {
      title: "Focus session",
      body: "A named resumable ignored attention workspace containing selected workstreams, presentation preferences, one active review turn, and one previous-review snapshot over the single repository model. It contains no unique project meaning.",
      state: "proposed", scope: { kind: "repository" }, introducedBy: "migration", sourceRefs: ["legacy-brainstorm:D-multiple-ephemeral-sessions-one-model"], relationships: [],
    }).id,
    addObject(model, "concepts", "review-turn", {
      title: "Review turn",
      body: "A user-facing oversight boundary that separates For awareness from exact Decisions needed. It is not a Git commit or authority transition.",
      state: "proposed", scope: { kind: "repository" }, introducedBy: "migration", sourceRefs: ["legacy-brainstorm:D-materiality-based-review-turns"], relationships: [],
    }).id,
  ];
  const derivedScenarioIds = [
    addObject(model, "scenarios", "fork-correction-continues-focus", {
      title: "Forking to correct an earlier response preserves focus",
      body: "When the user forks or clones a Pi conversation to edit an earlier response, the exact linked focus session is validated and remains active without requiring `/dag brainstorm` reentry.",
      kind: "ordinary", state: "proposed", scope: { kind: "repository" }, introducedBy: "migration", sourceRefs: ["legacy-brainstorm:D-inherit-exact-focus-across-fork"], relationships: [],
    }).id,
    addObject(model, "scenarios", "stale-sparse-review", {
      title: "Sparse review resolution preserves stale and omitted points",
      body: "When the user answers only part of a review and unrelated model content changed, each fresh hash-matching point resolves independently while omitted, ambiguous, contradictory, or stale points remain unresolved.",
      kind: "boundary", state: "proposed", scope: { kind: "repository" }, introducedBy: "migration", sourceRefs: ["legacy-brainstorm:D-point-level-hash-resolution"], relationships: [],
    }).id,
  ];

  const openQuestions = (legacy.questions ?? []).filter(({ status }) => status === "open");
  const evidenceIds = new Set(openQuestions.flatMap(({ evidenceIds = [] }) => evidenceIds));
  for (const item of (legacy.evidence ?? []).filter(({ id }) => evidenceIds.has(id))) {
    const object = addObject(model, "evidence", item.id, {
      title: item.source || item.title || item.key || item.id,
      body: item.summary || "Migrated legacy evidence.",
      state: "current",
      confidence: item.kind === "user" || item.kind === "repository" ? "high" : "medium",
      scope: { kind: "repository" },
      introducedBy: item.kind === "user" ? "user" : item.kind === "prototype" ? "prototype" : "migration",
      sourceRefs: [String(item.source || `legacy-brainstorm:${item.id}`)],
      relationships: [],
    });
    legacyToNew.set(item.id, object.id);
    mappings.push(mapped(item.id, "evidence", object.id, "Evidence is referenced by an unresolved migrated question."));
  }

  for (const question of openQuestions) {
    const workstreamIds = scopeFromQuestion(legacy, question, legacyToNew);
    const evidenceRelationships = (question.evidenceIds ?? []).map((id: string) => legacyToNew.get(id)).filter(Boolean).map((targetId: string) => ({ kind: "supports", targetId }));
    const object = addObject(model, "questions", question.id, {
      title: question.title ?? question.key ?? question.prompt,
      body: question.prompt,
      kind: question.kind === "contradiction" ? "contradiction" : "uncertainty",
      state: "deferred",
      scope: workstreamIds.length ? { kind: "workstreams", workstreamIds } : { kind: "repository" },
      introducedBy: "migration",
      sourceRefs: [`legacy-brainstorm:${question.id}`],
      relationships: evidenceRelationships,
    });
    legacyToNew.set(question.id, object.id);
    mappings.push(mapped(question.id, "questions", object.id, "Open legacy question remains deferred unresolved direction."));
  }

  const relevantQuestionIds = new Set(openQuestions.map(({ id }) => id));
  for (const proposal of (legacy.proposals ?? []).filter(({ status, questionIds = [] }) => typeof status === "string" && ["candidate", "recommended"].includes(status) && questionIds.some((id: string) => relevantQuestionIds.has(id)))) {
    const relatedQuestions = (proposal.questionIds ?? []).map((id: string) => legacyToNew.get(id)).filter(Boolean);
    const object = addObject(model, "proposals", proposal.id, {
      title: proposal.title ?? proposal.key ?? proposal.id,
      body: proposal.description ?? proposal.body ?? proposal.contract,
      rationale: proposal.rationale,
      state: proposal.status,
      scope: { kind: "repository" },
      introducedBy: "migration",
      sourceRefs: [`legacy-brainstorm:${proposal.id}`],
      relationships: relatedQuestions.map((targetId: string) => ({ kind: "addresses", targetId })),
    });
    legacyToNew.set(proposal.id, object.id);
    mappings.push(mapped(proposal.id, "proposals", object.id, "Candidate option remains attached to an unresolved question."));
  }

  const activeDecisions = (legacy.decisions ?? []).filter(({ status }) => status === "active");
  const startIndex = activeDecisions.findIndex(({ id }) => id === NEW_MODEL_DECISION_START);
  const deferredGroups = new Map<string, ModelObject>();
  for (let index = 0; index < activeDecisions.length; index++) {
    const decision = activeDecisions[index];
    const override = overrides[decision.id];
    const supersededReason = SUPERSEDED_LEGACY_DECISIONS.get(decision.id);
    if (supersededReason || override?.action === "omit") {
      mappings.push({ legacyId: decision.id, disposition: "omitted", reason: supersededReason ?? "Semantic audit classified this legacy contract as obsolete or conflicting." });
      continue;
    }
    if (override?.action === "defer") {
      const group = ["D46-plan-and-chunk-assign-prototypes", "D49-relevant-first-spec-traversal", "D-conductor-owned-reconciliation"].includes(decision.id) ? "planning-execution" : "turn-lavish-renderer";
      let object = deferredGroups.get(group);
      if (!object) {
        object = addObject(model, "discoveries", `deferred-${group}`, {
          title: group === "planning-execution" ? "Deferred model-aware planning and execution details" : "Deferred turn-to-Lavish renderer details",
          body: group === "planning-execution" ? "Planning, chunking, and execution integration details remain deferred until brainstorming is dogfooded." : "Renderer architecture, modality, controls, transport, threat model, and validation seams remain deferred; V1 is chat-only.",
          state: "deferred",
          scope: { kind: "repository" },
          introducedBy: "migration",
          sourceRefs: [],
          relationships: [],
        });
        deferredGroups.set(group, object);
      }
      object.body += `\n\n- ${decision.title}: ${decision.contract}`;
      object.legacyIds = [...new Set([...(object.legacyIds ?? []), decision.id])];
      object.sourceRefs = [...new Set([...object.sourceRefs, `legacy-brainstorm:${decision.id}`])];
      legacyToNew.set(decision.id, object.id);
      mappings.push(mapped(decision.id, "discoveries", object.id, "Renderer or planning detail remains a deferred non-governing discovery."));
      continue;
    }
    const collection: ModelCollectionName = startIndex >= 0 && index >= startIndex ? "decisions" : "commitments";
    const scopeIds = scopeFromDecision(legacy, decision, legacyToNew);
    const rewritten = override?.action === "rewrite";
    const value = collection === "decisions"
      ? {
          title: override?.title ?? decision.title,
          body: override?.body ?? decision.contract,
          rationale: rewritten ? "Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior." : decision.rationale,
          state: "candidate",
          selectedProposalIds: [],
          resolvesQuestionIds: [],
        }
      : {
          title: override?.title ?? decision.title,
          body: override?.body ?? decision.contract,
          rationale: rewritten ? "Semantic migration rewrite replacing obsolete legacy terminology while preserving the accepted behavior." : decision.rationale,
          state: "not_reviewed",
        };
    const object = addObject(model, collection, decision.id, {
      ...value,
      scope: scopeIds.length ? { kind: "workstreams", workstreamIds: scopeIds } : { kind: "repository" },
      introducedBy: "migration",
      sourceRefs: [`legacy-brainstorm:${decision.id}`],
      relationships: [],
    });
    legacyToNew.set(decision.id, object.id);
    mappings.push(mapped(decision.id, collection, object.id, collection === "decisions" ? "Shared-model-era accepted choice is a candidate decision pending cutover receipt." : rewritten ? "Legacy contract was semantically rewritten into model-native candidate commitment prose." : "Current behavioral commitment retained pending cutover receipt."));
  }

  for (const tangent of (legacy.tangents ?? []).filter(({ status }) => typeof status !== "string" || !["closed", "out_of_scope"].includes(status))) {
    const group = tangent.id === "T-turn-lavish-renderer" ? "turn-lavish-renderer" : ["T13-planning-handoff", "T-model-execution-reconciliation"].includes(tangent.id) ? "planning-execution" : undefined;
    const grouped = group ? deferredGroups.get(group) : undefined;
    if (grouped) {
      grouped.legacyIds = [...new Set([...(grouped.legacyIds ?? []), tangent.id])];
      grouped.sourceRefs = [...new Set([...grouped.sourceRefs, `legacy-brainstorm:${tangent.id}`])];
      legacyToNew.set(tangent.id, grouped.id);
      mappings.push(mapped(tangent.id, "discoveries", grouped.id, "Deferred tangent consolidated into the matching non-governing discovery."));
      continue;
    }
    const scopeIds = (tangent.neighborhoodIds ?? []).map((id: string) => legacyToNew.get(id)).filter(Boolean);
    const object = addObject(model, "discoveries", tangent.id, {
      title: tangent.title ?? tangent.id,
      body: tangent.explorationSummary ?? tangent.researchNeeded ?? tangent.whyNow ?? "Deferred legacy frontier.",
      state: "deferred",
      scope: scopeIds.length ? { kind: "workstreams", workstreamIds: scopeIds } : { kind: "repository" },
      introducedBy: "migration",
      sourceRefs: [`legacy-brainstorm:${tangent.id}`],
      relationships: [],
      implications: tangent.connection,
    });
    legacyToNew.set(tangent.id, object.id);
    mappings.push(mapped(tangent.id, "discoveries", object.id, "Non-closed tangent remains a deferred discovery/frontier item."));
  }

  for (const probe of legacy.probes ?? []) {
    const object = addObject(model, "discoveries", probe.id, {
      title: probe.title ?? probe.id,
      body: probe.result ?? probe.description ?? "Legacy prototype/probe finding.",
      state: probe.status === "completed" ? "integrated" : "deferred",
      scope: { kind: "repository" },
      introducedBy: "prototype",
      sourceRefs: [probe.artifactPath ?? `legacy-brainstorm:${probe.id}`],
      relationships: [],
    });
    legacyToNew.set(probe.id, object.id);
    mappings.push(mapped(probe.id, "discoveries", object.id, "Legacy probe retained as a prototype discovery."));
  }

  for (const item of legacy.evidence ?? []) {
    if (!legacyToNew.has(item.id)) mappings.push({ legacyId: item.id, disposition: "omitted", reason: "Not referenced by the current unresolved frontier; rationale is preserved in mapped decisions/contracts where useful." });
  }
  for (const question of legacy.questions ?? []) if (!legacyToNew.has(question.id)) mappings.push({ legacyId: question.id, disposition: "omitted", reason: "Answered/obsolete interaction question; current semantic outcome is preserved through mapped decisions." });
  for (const proposal of legacy.proposals ?? []) if (!legacyToNew.has(proposal.id)) mappings.push({ legacyId: proposal.id, disposition: "omitted", reason: "Rejected, superseded, accepted-into-decision, or otherwise non-current option." });
  for (const tangent of legacy.tangents ?? []) if (!legacyToNew.has(tangent.id)) mappings.push({ legacyId: tangent.id, disposition: "omitted", reason: "Closed or out-of-scope legacy exploration branch." });
  for (const promotion of legacy.promotions ?? []) mappings.push({ legacyId: promotion.id, disposition: "omitted", reason: "Promotion is obsolete workflow history; target specs are migration inputs." });

  const decisionIds = activeDecisions.map(({ id }) => legacyToNew.get(id)).filter(Boolean) as string[];
  const recentDecisionIds = startIndex >= 0 ? activeDecisions.slice(startIndex).map(({ id }) => legacyToNew.get(id)).filter(Boolean) as string[] : [];
  const legacyCommitmentIds = decisionIds.filter((id) => id.startsWith("COM-"));
  const projectionViews: SpecProjectionView[] = [
    {
      id: "SPEC-root",
      kind: "index",
      path: "spec/spec.md",
      title: "Product specifications",
      summary: "Current accepted project behavior generated from the shared project model.",
      childViewIds: ["SPEC-mixed-initiative", "SPEC-structured-brainstorming", "SPEC-prototypes"],
    },
    {
      id: "SPEC-mixed-initiative",
      kind: "spec",
      path: "spec/mixed-initiative-project-model/spec.md",
      title: "Mixed-initiative project model",
      summary: "Shared-model authority, turn-loop, projection, migration, and Pi integration contracts.",
      sections: [
        { id: "intent", title: "Candidate intent", objectIds: derivedIntentIds },
        { id: "concepts", title: "Candidate concepts", objectIds: derivedConceptIds },
        { id: "accepted-direction", title: "Candidate accepted direction", objectIds: recentDecisionIds },
        { id: "representative-scenarios", title: "Candidate representative scenarios", objectIds: derivedScenarioIds },
      ],
    },
    {
      id: "SPEC-structured-brainstorming",
      kind: "spec",
      path: "spec/structured-brainstorming/spec.md",
      title: "Structured brainstorming behavior",
      summary: "Retained research, interaction, question-quality, prototype, and validation behavior migrated as commitments.",
      sections: [{ id: "behavioral-contracts", title: "Candidate behavioral contracts", objectIds: legacyCommitmentIds }],
    },
    {
      id: "SPEC-prototypes",
      kind: "prototype_index",
      path: "spec/prototypes/spec.md",
      title: "Behavioral prototypes",
      summary: "Hand-authored executable references linked as evidence; prototype behavior is never authority by itself.",
      manualLinks: [
        { path: "spec/prototypes/object-tools/README.md", title: "Object-tools prototype", summary: "Legacy five-tool behavioral reference and migration input." },
        { path: "spec/prototypes/brainstorm-pi-adapter/README.md", title: "Pi adapter prototype", summary: "Legacy command/mode integration evidence." },
      ],
    },
  ];
  model.project.projections.specs = projectionViews;

  if (legacy.currentUnderstanding?.body) {
    const sourceIds = (legacy.currentUnderstanding.source?.decisionIds ?? []).map((id: string) => legacyToNew.get(id)).filter(Boolean) as string[];
    model.project.currentUnderstanding = {
      body: legacy.currentUnderstanding.body,
      generatedAt: legacy.currentUnderstanding.updatedAt ?? new Date().toISOString(),
      sourceObjects: sourceIds.map((id) => {
        const found = findMappedObject(model, id)!;
        return { id, semanticHash: semanticHash(found.collection, found.object) };
      }),
    };
  }

  const specFiles = await listSpecMarkdown(root);
  warnings.push("Candidate objects intentionally have no human acceptance receipts; cutover acceptance must bind their final semantic hashes.");
  warnings.push("Seven intent/concept/scenario objects were semantically derived from accepted legacy decisions and require explicit audit alongside one-to-one mappings.");
  warnings.push("Generated candidate specs restructure legacy supporting documents; semantic coverage must be reviewed before those files are retired.");
  const report = renderMigrationReport(model, mappings, warnings, specFiles);
  return { model, mappings, warnings, report };
}

function addObject(model: ProjectModel, collection: ModelCollectionName, legacyId: string, value: Record<string, unknown>): ModelObject {
  const id = allocateObjectId(model, collection, legacyId.replace(/^[A-Z0-9]+-/, ""));
  const timestamp = new Date().toISOString();
  const object = {
    ...value,
    id,
    title: String(value.title ?? legacyId),
    body: String(value.body ?? legacyId),
    createdAt: timestamp,
    updatedAt: timestamp,
    legacyIds: [legacyId],
  } as unknown as ModelObject;
  (model[collection] as ModelObject[]).push(object);
  return object;
}

function mapped(legacyId: string, targetCollection: ModelCollectionName, targetId: string, reason: string): MigrationMapping {
  return { legacyId, targetCollection, targetId, disposition: "mapped", reason };
}

function scopeFromQuestion(legacy: LegacyState, question: LegacyEntity, mapping: Map<string, string>): string[] {
  return [...new Set((question.tangentIds ?? []).flatMap((tangentId: string) => {
    const tangent = legacy.tangents.find(({ id }) => id === tangentId);
    return (tangent?.neighborhoodIds ?? []).map((id: string) => mapping.get(id)).filter(Boolean);
  }))] as string[];
}

function scopeFromDecision(legacy: LegacyState, decision: LegacyEntity, mapping: Map<string, string>): string[] {
  const ids = (decision.questionIds ?? []).flatMap((questionId: string) => {
    const question = legacy.questions.find(({ id }) => id === questionId);
    return question ? scopeFromQuestion(legacy, question, mapping) : [];
  }) as string[];
  return [...new Set<string>(ids)];
}

function findMappedObject(model: ProjectModel, id: string): { collection: ModelCollectionName; object: ModelObject } | undefined {
  for (const collection of Object.keys(model).filter((key) => Array.isArray((model as any)[key])) as ModelCollectionName[]) {
    const object = (model[collection] as ModelObject[]).find((candidate) => candidate.id === id);
    if (object) return { collection, object };
  }
  return undefined;
}

async function listSpecMarkdown(root: string): Promise<string[]> {
  const output: string[] = [];
  const walk = async (directory: string) => {
    let entries;
    try { entries = await readdir(directory, { withFileTypes: true }); } catch { return; }
    for (const entry of entries) {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) await walk(path);
      else if (entry.isFile() && entry.name.endsWith(".md")) output.push(path.slice(`${resolve(root)}/`.length));
    }
  };
  await walk(resolve(root, "spec"));
  return output.sort();
}

function renderMigrationReport(model: ProjectModel, mappings: MigrationMapping[], warnings: string[], specFiles: string[]): string {
  const mappedCounts = mappings.filter(({ disposition }) => disposition === "mapped").reduce<Record<string, number>>((counts, item) => {
    counts[item.targetCollection!] = (counts[item.targetCollection!] ?? 0) + 1;
    return counts;
  }, {});
  const lines = [
    "# Shared project-model migration candidate",
    "",
    "Status: non-authoritative candidate pending semantic audit and explicit cutover acceptance.",
    "",
    "## Candidate summary",
    "",
    `- Project: ${model.project.title}`,
    `- Mode: ${model.project.mode}`,
    `- Candidate manifest hash: \`${candidateManifestHash(model)}\``,
    `- Mapped objects: ${mappings.filter(({ disposition }) => disposition === "mapped").length}`,
    `- Omitted legacy objects: ${mappings.filter(({ disposition }) => disposition === "omitted").length}`,
    `- Existing specification Markdown inputs: ${specFiles.length}`,
    ...Object.entries(mappedCounts).sort().map(([collection, count]) => `- ${collection}: ${count}`),
    "",
    "## Required audit",
    "",
    "1. Confirm candidate intent/decision/commitment classification.",
    "2. Confirm every omitted active contract is truly superseded or redundant.",
    "3. Compare generated candidate specs with each current functional/supporting spec.",
    "4. Resolve contradictory or missing behavior before creating migration-cutover receipts.",
    "5. Confirm prototype links remain evidence rather than authority.",
    "",
    "## Warnings",
    "",
    ...warnings.map((warning) => `- ${warning}`),
    "",
    "## Existing spec inputs",
    "",
    ...specFiles.map((path) => `- \`${path}\``),
    "",
    "## Object mapping",
    "",
    "| Legacy ID | Disposition | Candidate target | Reason |",
    "|---|---|---|---|",
    ...mappings.sort((a, b) => a.legacyId.localeCompare(b.legacyId)).map((item) => `| \`${item.legacyId}\` | ${item.disposition} | ${item.targetId ? `\`${item.targetCollection}/${item.targetId}\`` : "—"} | ${escapeTable(item.reason)} |`),
    "",
  ];
  return `${lines.join("\n")}\n`;
}

async function loadMigrationOverrides(root: string): Promise<Record<string, { action: "omit" | "defer" | "rewrite" | "retain"; title?: string; body?: string }>> {
  const path = resolve(root, "project-model/migrations/brainstorm-v2-overrides.json");
  try { return JSON.parse(await readFile(path, "utf8")); }
  catch (error: any) {
    if (error?.code === "ENOENT") return {};
    throw new Error(`Invalid migration overrides ${path}: ${error.message}`);
  }
}

function escapeTable(value: string): string { return value.replaceAll("|", "\\|").replaceAll("\n", " "); }
