import { resolve } from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { truncateHead, withFileMutationQueue } from "@earendil-works/pi-coding-agent";
import { StringEnum } from "@earendil-works/pi-ai";
import { Type } from "typebox";
import { ProjectModelDomain } from "./domain.ts";
import { bootstrapProjectMigration } from "./migration-workflow.ts";
import { ReviewPresentationManager, type PresentationUpdate } from "./review-presentation.ts";

export const MODEL_TOOL_NAMES = [
  "dag_model_context",
  "dag_model_update",
  "dag_model_record_direction",
  "dag_model_review",
  "dag_model_present_review",
  "dag_model_resolve_review",
  "dag_model_specs",
] as const;

const MODEL_COLLECTION_VALUES = [
  "workstreams", "intents", "concepts", "evidence", "assumptions", "questions", "tensions", "scenarios", "proposals", "decisions", "commitments", "discoveries",
] as const;
const KICKOFF_MESSAGE = "dag-model-kickoff";

const VALUE_SCHEMA = Type.Record(Type.String(), Type.Unknown());
const ADD_SCHEMA = Type.Object({
  collection: StringEnum(MODEL_COLLECTION_VALUES),
  id: Type.Optional(Type.String()),
  key: Type.Optional(Type.String()),
  value: VALUE_SCHEMA,
});
const PATCH_SCHEMA = Type.Object({ id: Type.String(), changes: VALUE_SCHEMA });
const UNDERSTANDING_SCHEMA = Type.Object({ body: Type.String(), sourceObjectIds: Type.Array(Type.String()) });
const DIRECTION_SCHEMA = Type.Object({
  collection: StringEnum(["intents", "concepts", "scenarios", "decisions", "commitments"] as const),
  id: Type.Optional(Type.String()),
  newId: Type.Optional(Type.String()),
  key: Type.Optional(Type.String()),
  state: Type.Optional(Type.String()),
  value: Type.Optional(VALUE_SCHEMA),
});
const OPTION_SCHEMA = Type.Object({
  id: Type.Optional(Type.String()), key: Type.Optional(Type.String()), label: Type.String(), description: Type.String(), objectId: Type.Optional(Type.String()), recommended: Type.Optional(Type.Boolean()), rationale: Type.Optional(Type.String()), direction: Type.Optional(DIRECTION_SCHEMA),
});
const REVIEW_POINT_SCHEMA = Type.Object({
  id: Type.Optional(Type.String()), key: Type.Optional(Type.String()), title: Type.String(), context: Type.String(), purpose: StringEnum(["awareness", "decision"] as const), question: Type.Optional(Type.String()), objectIds: Type.Optional(Type.Array(Type.String())), options: Type.Optional(Type.Array(OPTION_SCHEMA)), rejectDirection: Type.Optional(DIRECTION_SCHEMA), deferDirection: Type.Optional(DIRECTION_SCHEMA),
});

const SCOPE_SCHEMA = Type.Object({ workstreamIds: Type.Array(Type.String()), objectIds: Type.Optional(Type.Array(Type.String())) });

export function registerProjectModelIntegration(pi: ExtensionAPI) {
  const domainByRoot = new Map<string, ProjectModelDomain>();
  const presenterByRoot = new Map<string, ReviewPresentationManager>();
  const domain = (cwd: string) => {
    const root = resolve(cwd);
    let value = domainByRoot.get(root);
    if (!value) { value = new ProjectModelDomain(root); domainByRoot.set(root, value); }
    return value;
  };
  const presenter = (cwd: string) => {
    const root = resolve(cwd);
    let value = presenterByRoot.get(root);
    if (!value) { value = new ReviewPresentationManager(root); presenterByRoot.set(root, value); }
    return value;
  };
  const modelQueue = async <T>(ctx: any, callback: () => Promise<T>) => withFileMutationQueue(domain(ctx.cwd).models.path, callback);
  const updateFields = {
    add: Type.Optional(Type.Array(ADD_SCHEMA)), patch: Type.Optional(Type.Array(PATCH_SCHEMA)), removeIds: Type.Optional(Type.Array(Type.String())),
    currentUnderstanding: Type.Optional(UNDERSTANDING_SCHEMA), specViews: Type.Optional(Type.Array(VALUE_SCHEMA)), migration: Type.Optional(VALUE_SCHEMA),
  };
  registerTool(pi, {
    name: "dag_model_context", label: "Model Context",
    description: "Read scoped model context, discover multiple pending reviews, or read an exact review. No saved focus is used. Historical reviews require their exact legacyFocusId and reviewId and are read-only.",
    parameters: Type.Object({ scope: SCOPE_SCHEMA, view: Type.Optional(StringEnum(["orientation", "migration", "entities", "frontier", "reviews", "review", "legacy_review", "governing"] as const)), ids: Type.Optional(Type.Array(Type.String())), reviewId: Type.Optional(Type.String()), legacyFocusId: Type.Optional(Type.String()) }),
    execute: async (params: any, ctx: any) => asToolResult(await domain(ctx.cwd).context(params.scope, params), "context"),
  });
  registerTool(pi, {
    name: "dag_model_update", label: "Model Update",
    description: "Record scoped non-governing findings, metadata, relationships or Current understanding. Use record_direction for context-informed explicit user direction, not fabricated consent receipts.",
    parameters: Type.Object({ scope: SCOPE_SCHEMA, ...updateFields }),
    execute: async (params: any, ctx: any) => modelQueue(ctx, async () => asToolResult(await domain(ctx.cwd).update(params.scope, params), "update")),
  });
  registerTool(pi, {
    name: "dag_model_record_direction", label: "Record User Direction",
    description: "Record explicit user direction understood in conversation context. Chat and Lavish are equally valid sources; any user text is not blanket permission. No semantic hash or receipt is required. Cutover is isolated and retains structural/file safety checks.",
    parameters: Type.Object({ scope: SCOPE_SCHEMA, directions: Type.Optional(Type.Array(DIRECTION_SCHEMA, { minItems: 1 })), currentUnderstanding: Type.Optional(UNDERSTANDING_SCHEMA), specViews: Type.Optional(Type.Array(VALUE_SCHEMA)), cutover: Type.Optional(Type.Boolean()) }),
    execute: async (params: any, ctx: any) => modelQueue(ctx, async () => asToolResult(await domain(ctx.cwd).recordDirection(params.scope, params), "record_direction")),
  });
  registerTool(pi, {
    name: "dag_model_review", label: "Model Review",
    description: "Create an independently addressed durable review of relevant unresolved decisions. Multiple reviews may remain pending; creating one never replaces another. Present its decisions before interpreting outcomes.",
    parameters: Type.Object({ scope: SCOPE_SCHEMA, id: Type.Optional(Type.String()), key: Type.Optional(Type.String()), title: Type.String(), points: Type.Array(REVIEW_POINT_SCHEMA, { minItems: 1 }) }),
    execute: async (params: any, ctx: any) => modelQueue(ctx, async () => asToolResult(await domain(ctx.cwd).createReview(params.scope, params), "review")),
  });
  registerTool(pi, {
    name: "dag_model_present_review", label: "Present Model Review",
    description: "Optionally present an exact review in Lavish, collect submitted feedback, resume waiting, or end presentation. Artifact digests identify derived cache versions, never consent. Presentation does not resolve decisions.",
    parameters: Type.Object({ reviewId: Type.String(), action: Type.Optional(StringEnum(["present", "collect", "resume", "end"] as const)), presentationBlocks: Type.Optional(Type.Array(VALUE_SCHEMA)), reopen: Type.Optional(Type.Boolean()) }),
    execute: async (params: any, ctx: any, runtime: any) => {
      const projection = await domain(ctx.cwd).reviewTurn(params.reviewId, params.presentationBlocks ?? []);
      const manager = presenter(ctx.cwd);
      const onUpdate = (event: PresentationUpdate) => runtime.onUpdate?.({ content: [{ type: "text", text: JSON.stringify(event) }], details: { reviewId: params.reviewId, phase: event.phase } });
      const options = { signal: runtime.signal, onUpdate, reopen: params.reopen };
      const action = params.action ?? "present";
      const result = action === "end" ? await manager.end(projection, runtime.signal) : action === "collect" ? await manager.collect(projection, options) : action === "resume" ? await manager.resume(projection, options) : await manager.present(projection, options);
      return asToolResult({ action: `present_review.${action}`, reviewId: params.reviewId, artifactPath: result.paths.html, status: result.metadata.status, feedback: result.feedback }, `present_review.${action}`);
    },
  });
  registerTool(pi, {
    name: "dag_model_resolve_review", label: "Resolve Model Review",
    description: "Resolve only explicit context-informed outcomes by exact review ID and CAS revision. Omitted/ambiguous decisions remain pending. No ephemeral interaction marker or hash claims semantic consent.",
    parameters: Type.Object({ reviewId: Type.String(), expectedRevision: Type.Integer({ minimum: 0 }), outcomes: Type.Optional(Type.Array(Type.Object({ pointId: Type.String(), action: StringEnum(["accept", "reject", "modify", "defer", "unresolved"] as const), optionId: Type.Optional(Type.String()), direction: Type.Optional(DIRECTION_SCHEMA) }))), update: Type.Optional(Type.Object(updateFields)), currentUnderstanding: Type.Optional(UNDERSTANDING_SCHEMA) }),
    execute: async (params: any, ctx: any) => modelQueue(ctx, async () => asToolResult(await domain(ctx.cwd).resolveReview(params), "resolve_review")),
  });
  registerTool(pi, {
    name: "dag_model_specs", label: "Model Specifications", description: "Preview, check, or regenerate deterministic specifications from governing model state.",
    parameters: Type.Object({ action: StringEnum(["preview", "check", "generate"] as const) }),
    execute: async (params: any, ctx: any) => modelQueue(ctx, async () => asToolResult(await domain(ctx.cwd).specs(params), `specs.${params.action}`)),
  });
  pi.on("session_start", () => { pi.setActiveTools([...new Set([...pi.getActiveTools(), ...MODEL_TOOL_NAMES])]); });
  pi.on("before_agent_start", (event: any) => ({ systemPrompt: `${event.systemPrompt}\n\n${MODEL_GUIDANCE}` }));
  return {
    handleMigrateCommand: async (args: string, ctx: any) => {
      if (args.trim()) { ctx.ui.notify("Usage: /dag migrate", "error"); return true; }
      const result = await bootstrapProjectMigration(ctx.cwd);
      pi.sendMessage({ customType: KICKOFF_MESSAGE, content: `Candidate ${result.created ? "created" : "resumed"}; inventoried ${result.sourceCount} sources and ${result.artifactCount} artifacts. ${MIGRATION_GUIDANCE}`, display: true }, { triggerTurn: true, deliverAs: "followUp" });
      return true;
    },
    handleBrainstormCommand: async (args: string, _ctx: any) => {
      pi.sendMessage({ customType: KICKOFF_MESSAGE, content: `Explore the user's request in the existing conversation context using available research and model tools. User topic: ${args.trim() || "continue the current topic"}. Choose explicit object/workstream scope from context; do not create or resume a durable focus. Review relevant unresolved decisions, without re-requesting already-settled direction.`, display: true }, { triggerTurn: true, deliverAs: "followUp" });
      return true;
    },
  };
}

function registerTool(pi: ExtensionAPI, definition: { name: string; label: string; description: string; parameters: any; execute: (params: any, ctx: any, runtime: { signal?: AbortSignal; onUpdate?: (update: any) => void }) => Promise<any> }) {
  pi.registerTool({
    name: definition.name,
    label: definition.label,
    description: definition.description,
    parameters: definition.parameters,
    async execute(_toolCallId, params, signal, onUpdate, ctx) { return definition.execute(params, ctx, { signal, onUpdate }); },
  });
}

function asToolResult(value: any, action: string) {
  const text = typeof value === "string" ? value : JSON.stringify(value);
  const truncated = truncateHead(text, { maxLines: 500, maxBytes: 30_000 });
  return {
    content: [{ type: "text", text: truncated.content }],
    details: { action, ...(value && typeof value === "object" ? compactDetails(value) : {}) },
  };
}

function compactDetails(value: any) {
  return {
    revision: value.revision,
    modelHash: value.modelHash,
    changedIds: value.changedIds,
    generatedPaths: value.generatedPaths ?? value.changedPaths,
    staleIds: value.stalePointIds,
    driftPaths: value.driftPaths,
    stalePaths: value.stalePaths ?? value.staleGeneratedPaths,
    reviewId: value.reviewId,
    reviewRevision: value.reviewRevision ?? value.review?.revision,
    artifactPath: value.artifactPath,
    status: value.status,
    promptCount: value.feedback?.prompts?.length,
    feedbackTruncated: value.feedback?.truncation?.truncated,
  };
}

const MODEL_GUIDANCE = `Model tools are always available with explicit object/workstream scope; there is no durable focus or activity mode.
- Read narrow model context before acting; research repository/external evidence before asking.
- Write coherent sourced discoveries, evidence, assumptions, questions, tensions, scenarios, and proposals immediately with dag_model_update; never use it to grant authority.
- Use dag_model_record_direction only for unambiguous direct user direction understood from the available conversation context, including prior settled intent. A question or any arbitrary user text is not blanket authorization; never invent approval markers or acceptance receipts. Agent-derived implications remain non-authoritative.
- Reviews are required for relevant unresolved decisions; discover pending IDs with dag_model_context reviews before proceeding. Initiate broad review turns only on material triggers. Separate For awareness from exact Decisions needed. Use only real alternatives.
- Write question briefs as scannable Markdown. Establish the decision, why it matters, the recommendation, and the exact question; adaptively add current behavior, change context, constraints, consequences, uncertainty, and model evidence when they support an informed choice.
- Keep Current understanding causal, current, explicitly non-authoritative, and grounded in exact object refs. Use adaptive Markdown to expose the goal, relevant current state or mechanism, and governing direction, adding other sections only when useful.
- Demonstrate comprehension through selective causal synthesis rather than reproducing the project model or filling an exhaustive template. Do not require a distinct formal acknowledgement surface.
- Resolve sparse responses independently through dag_model_resolve_review; stale, omitted, ambiguous, or conflicting points remain open. After a frontier review resolves, automatically explore and present the next supported material frontier until none remains, the user redirects, or unresolved user input blocks progress; never invent questions to claim exhaustion.
- Low-risk local prototypes are allowed when useful to answer the user's question and within scope; assess destructive, network, credential and external effects separately. Model context does not ban planning or execution. Before dispatch, review relevant unresolved questions/directions and pending reviews; surface genuine unresolved decisions, using existing conversation for already-settled meaning. Lavish is optional and user intent is channel-neutral.`;

const MIGRATION_GUIDANCE = `You are in guided project-model migration mode for an existing repository.
- Start with dag_model_context orientation. Inspect relevant repository evidence in tiers: supported legacy state; repository/package orientation; README, specs, docs, ADRs, and plans; tests or representative code when they confirm or contradict behavior; bounded Git history only for material ambiguity.
- Build a coherent non-authoritative candidate with dag_model_update. Agents classify meaning; tools own IDs, structural validation, CAS, and persistence. Never copy every file or mistake implementation detail for governing product direction.
- Keep project.migration current through dag_model_update migration. Classify every inventoried source as mapped, retained, or omitted with rationale. Classify every relevant artifact as create_generated, replace_generated, retain_reference, retain_evidence, or block. The tool records internal file digests for safe replacement, not semantic acceptance.
- A partial candidate may be presented, but set migration phase ready only when the model and projections are coherent, every material source and artifact is dispositioned, authority conflicts are resolved, and blockers are empty. Never report a synthetic completeness percentage.
- Before cutover, create one review with an awareness summary and a decision asking whether to cut over or continue refining/coexisting. Its options may omit semantic direction payloads because cutover is an isolated authority operation. Present it in chat or optionally with dag_model_present_review and rich blocks that show inferred goals/direction, workstreams and counts, unresolved questions, source mappings/omissions, generated-spec diffs, blockers, and an artifact-disposition table.
- Interpret genuine user-originated direction from chat, Lavish or other channels in context; a message is not blanket permission. If the user chooses cutover, call dag_model_record_direction with cutover: true and explicit scope. If the user asks for side-by-side artifacts or changes, resolve that directionless migration option with dag_model_resolve_review, update dispositions/model meaning, and present a refreshed review instead. Never cut over while migration readiness or file freshness fails.
- Physical coexistence is allowed; dual semantic authority is not. Retained specs remain linked references or evidence. If an existing artifact must remain governing, keep a blocker and do not cut over.
- Migration is not an activity-mode ban: planning and low-risk exploration may continue. Saving executable plans still requires accepted authoritative source meaning; candidate proposals alone do not become governing direction.`;
