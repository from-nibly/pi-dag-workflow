import { resolve } from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { truncateHead, withFileMutationQueue } from "@earendil-works/pi-coding-agent";
import { StringEnum } from "@earendil-works/pi-ai";
import { Type } from "typebox";
import { ProjectModelDomain, type DirectionInput, type ModelUpdateInput, type ReviewOutcomeInput, type ReviewPointInput } from "./domain.ts";
import type { LavishFeedback } from "./lavish-cli.ts";
import { bootstrapProjectMigration } from "./migration-workflow.ts";
import { sha256 } from "./model.ts";
import { ReviewPresentationManager, type PresentationUpdate } from "./review-presentation.ts";
import { normalizeFocusId } from "./sessions.ts";
import type { PresentationBlock } from "./review-turn.ts";
import { DEFAULT_PROJECT_MODEL_PATH, type ModelCollectionName } from "./types.ts";

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
const LINK_ENTRY = "dag-model-focus-link";
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

interface LinkEntry { repositoryRoot: string; focusSessionId: string; mode: "active" | "suspended" }

type CommandContext = any;

export function registerProjectModelIntegration(pi: ExtensionAPI) {
  let activeFocus: { id: string; repositoryRoot: string; mode: "brainstorm" | "migration" } | undefined;
  let modeActive = false;
  let currentInteractionRef: string | undefined;
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

  const deactivate = () => {
    modeActive = false;
    activeFocus = undefined;
    pi.setActiveTools(pi.getActiveTools().filter((name) => !MODEL_TOOL_NAMES.includes(name as any)));
  };
  const activate = (cwd: string, focusId: string, mode: "brainstorm" | "migration" = "brainstorm") => {
    activeFocus = { id: focusId, repositoryRoot: resolve(cwd), mode };
    modeActive = true;
    pi.setActiveTools([...new Set([...pi.getActiveTools(), ...MODEL_TOOL_NAMES])]);
  };
  const appendLink = (cwd: string, focusSessionId: string, mode: LinkEntry["mode"]) => {
    pi.appendEntry(LINK_ENTRY, { repositoryRoot: resolve(cwd), focusSessionId, mode } satisfies LinkEntry);
  };
  const requireFocus = (ctx: any) => {
    if (!modeActive || !activeFocus) throw new Error("No active model brainstorming focus. Run /dag brainstorm first.");
    if (activeFocus.repositoryRoot !== resolve(ctx.cwd)) throw new Error("Active model focus belongs to a different repository; run /dag brainstorm in this repository.");
    return activeFocus.id;
  };
  const modelQueue = async <T>(ctx: any, callback: () => Promise<T>) =>
    withFileMutationQueue(domain(ctx.cwd).models.path, callback);

  registerTool(pi, {
    name: "dag_model_context",
    label: "Model Context",
    description: "Read a narrow project-model or active-focus projection without mutating it.",
    parameters: Type.Object({ view: Type.Optional(StringEnum(["orientation", "migration", "entities", "frontier", "delta", "review", "governing"] as const)), ids: Type.Optional(Type.Array(Type.String())) }),
    execute: async (params: any, ctx: any) => asToolResult(await domain(ctx.cwd).context(requireFocus(ctx), params), "context"),
  });

  registerTool(pi, {
    name: "dag_model_update",
    label: "Model Update",
    description: "Atomically record non-authoritative findings, metadata, relationships, routing, or Current understanding. Cannot grant authority.",
    parameters: Type.Object({
      add: Type.Optional(Type.Array(ADD_SCHEMA)),
      patch: Type.Optional(Type.Array(PATCH_SCHEMA)),
      removeIds: Type.Optional(Type.Array(Type.String())),
      currentUnderstanding: Type.Optional(UNDERSTANDING_SCHEMA),
      specViews: Type.Optional(Type.Array(VALUE_SCHEMA)),
      migration: Type.Optional(VALUE_SCHEMA),
      focus: Type.Optional(Type.Object({ workstreamIds: Type.Array(Type.String()) })),
    }),
    execute: async (params: any, ctx: any) => modelQueue(ctx, async () => asToolResult(await domain(ctx.cwd).update(requireFocus(ctx), params as ModelUpdateInput), "update")),
  });

  registerTool(pi, {
    name: "dag_model_record_direction",
    label: "Record User Direction",
    description: "Record unambiguous direct user authority with content-bound receipts. Never use for agent-derived implications.",
    parameters: Type.Object({ directions: Type.Optional(Type.Array(DIRECTION_SCHEMA, { minItems: 1 })), currentUnderstanding: Type.Optional(UNDERSTANDING_SCHEMA), specViews: Type.Optional(Type.Array(VALUE_SCHEMA)), cutover: Type.Optional(Type.Object({ candidateManifestHash: Type.String() })) }),
    execute: async (params: any, ctx: any) => modelQueue(ctx, async () => {
      if (!currentInteractionRef) throw new Error("No eligible current user interaction for direct direction");
      const focusId = requireFocus(ctx);
      const activeReviewId = params.cutover ? (await domain(ctx.cwd).sessions.load(focusId)).activeReview?.id : undefined;
      const result = await domain(ctx.cwd).recordDirection(focusId, params, currentInteractionRef);
      if (params.cutover) {
        if (activeReviewId) await presenter(ctx.cwd).cleanup(focusId, activeReviewId).catch(() => undefined);
        if (activeFocus?.id === focusId && activeFocus.repositoryRoot === resolve(ctx.cwd)) activeFocus.mode = "brainstorm";
      }
      return asToolResult(result, "record_direction");
    }),
  });

  registerTool(pi, {
    name: "dag_model_review",
    label: "Model Review Turn",
    description: "Create one hash-bound For awareness / Decisions needed review turn for exact chat presentation.",
    parameters: Type.Object({ id: Type.Optional(Type.String()), key: Type.Optional(Type.String()), title: Type.String(), points: Type.Array(REVIEW_POINT_SCHEMA, { minItems: 1 }) }),
    execute: async (params: any, ctx: any) => modelQueue(ctx, async () => {
      const focusId = requireFocus(ctx);
      const result = await domain(ctx.cwd).createReview(focusId, params as { title: string; points: ReviewPointInput[] });
      const presented = await domain(ctx.cwd).markReviewPresented(focusId, result.review.id, result.reviewHash);
      if (!presented) throw new Error("Exact visible review tool result could not be marked presented");
      return {
        content: [{ type: "text", text: result.markdown }],
        details: { action: result.action, revision: result.revision, modelHash: result.modelHash, focusId: result.focusId, reviewId: result.review.id, reviewHash: result.reviewHash, presented: true },
      };
    }),
  });

  registerTool(pi, {
    name: "dag_model_present_review",
    label: "Present Model Review",
    description: "Render and present the active hash-bound model review through the optional Lavish adapter without mutating semantic outcomes.",
    parameters: Type.Object({
      action: Type.Optional(StringEnum(["present", "resume", "end"] as const)),
      presentationBlocks: Type.Optional(Type.Array(VALUE_SCHEMA)),
      reopen: Type.Optional(Type.Boolean()),
    }),
    execute: async (params: any, ctx: any, runtime: any) => {
      const focusId = requireFocus(ctx);
      const projection = await domain(ctx.cwd).reviewTurn(focusId, (params.presentationBlocks ?? []) as PresentationBlock[]);
      const manager = presenter(ctx.cwd);
      const onUpdate = (event: PresentationUpdate) => runtime.onUpdate?.({
        content: [{ type: "text", text: JSON.stringify({ action: "present_review", reviewId: projection.review.id, ...event }) }],
        details: { action: "present_review", focusId, reviewId: projection.review.id, phase: event.phase },
      });
      const onPresented = async () => {
        const marked = await domain(ctx.cwd).markReviewPresented(focusId, projection.review.id, projection.review.semanticHash);
        if (!marked) throw new Error("Active review changed before Lavish presentation completed");
      };
      const action = params.action ?? "present";
      if (action === "end") return asToolResult(await manager.end(projection, runtime.signal), "present_review.end");
      const result = action === "resume"
        ? await manager.resume(projection, { signal: runtime.signal, onUpdate, onPresented, reopen: params.reopen })
        : await manager.present(projection, { signal: runtime.signal, onUpdate, onPresented });
      if (result.feedback.prompts.length) {
        const current = await domain(ctx.cwd).reviewTurn(focusId);
        if (current.review.id !== projection.review.id || current.review.semanticHash !== projection.review.semanticHash) throw new Error("Lavish feedback no longer matches the active review");
        currentInteractionRef = lavishFeedbackInteractionRef(projection.review.id, projection.review.semanticHash, result.feedback);
      }
      return asToolResult({ action: `present_review.${action}`, focusId, reviewId: projection.review.id, artifactPath: result.paths.html, status: result.metadata.status, feedback: result.feedback }, `present_review.${action}`);
    },
  });

  registerTool(pi, {
    name: "dag_model_resolve_review",
    label: "Resolve Model Review",
    description: "Apply fresh independent semantic outcomes from the active review; stale, omitted, and ambiguous points remain unresolved.",
    parameters: Type.Object({
      reviewId: Type.Optional(Type.String()),
      outcomes: Type.Optional(Type.Array(Type.Object({ pointId: Type.String(), action: StringEnum(["accept", "reject", "modify", "defer", "unresolved"] as const), optionId: Type.Optional(Type.String()), direction: Type.Optional(DIRECTION_SCHEMA) }))),
      update: Type.Optional(Type.Object({ add: Type.Optional(Type.Array(ADD_SCHEMA)), patch: Type.Optional(Type.Array(PATCH_SCHEMA)), removeIds: Type.Optional(Type.Array(Type.String())), currentUnderstanding: Type.Optional(UNDERSTANDING_SCHEMA), specViews: Type.Optional(Type.Array(VALUE_SCHEMA)), migration: Type.Optional(VALUE_SCHEMA) })),
      currentUnderstanding: Type.Optional(UNDERSTANDING_SCHEMA),
    }),
    execute: async (params: any, ctx: any) => modelQueue(ctx, async () => {
      if (!currentInteractionRef) throw new Error("No eligible current user interaction for review resolution");
      const focusId = requireFocus(ctx);
      const result = await domain(ctx.cwd).resolveReview(focusId, params as { outcomes?: ReviewOutcomeInput[] }, currentInteractionRef);
      if (!result.remainingReview) await presenter(ctx.cwd).cleanup(focusId, result.reviewId).catch(() => undefined);
      if (result.remainingReview) {
        return { content: [{ type: "text", text: result.remainingReview.markdown }], details: { ...compactDetails(result), action: "resolve_review", unresolvedPointIds: result.unresolvedPointIds } };
      }
      return asToolResult(result, "resolve_review");
    }),
  });

  registerTool(pi, {
    name: "dag_model_specs",
    label: "Model Specifications",
    description: "Preview, check, or explicitly regenerate deterministic current specifications from the project model.",
    parameters: Type.Object({ action: StringEnum(["preview", "check", "generate"] as const) }),
    execute: async (params: any, ctx: any) => modelQueue(ctx, async () => asToolResult(await domain(ctx.cwd).specs(params), `specs.${params.action}`)),
  });

  pi.on("input", (event: any, ctx: any) => {
    if (event.source === "extension") return;
    currentInteractionRef = `pi-input:${ctx.sessionManager.getSessionId?.() ?? "session"}:${Date.now()}`;
  });

  pi.on("session_start", async (_event: any, ctx: any) => {
    deactivate();
    const link = latestFocusLink(ctx, resolve(ctx.cwd));
    if (!link || link.mode !== "active") return;
    try {
      const projectDomain = domain(ctx.cwd);
      await projectDomain.reconcileSatisfiedReview(link.focusSessionId);
      const focus = await projectDomain.sessions.load(link.focusSessionId);
      if (focus.status !== "active") return;
      const model = await projectDomain.models.load();
      activate(ctx.cwd, link.focusSessionId, model.project.migration?.focusId === focus.id && model.project.mode === "candidate" ? "migration" : "brainstorm");
    } catch {
      ctx.ui.notify(`Model focus ${link.focusSessionId} could not be restored; run /dag brainstorm resume.`, "warning");
    }
  });

  pi.on("before_agent_start", (event: any, ctx: any) => {
    if (!modeActive || !activeFocus || activeFocus.repositoryRoot !== resolve(ctx.cwd)) return;
    const guidance = activeFocus.mode === "migration" ? MIGRATION_MODE_GUIDANCE : MODEL_MODE_GUIDANCE;
    return {
      systemPrompt: `${event.systemPrompt}\n\n${guidance}\nActive focus session: ${activeFocus.id}`,
    };
  });

  pi.on("turn_end", async (event: any, ctx: any) => {
    if (!activeFocus || activeFocus.repositoryRoot !== resolve(ctx.cwd)) return;
    const focus = await domain(ctx.cwd).sessions.load(activeFocus.id);
    if (!focus.activeReview || focus.activeReview.presentedAt) return;
    const projected = await domain(ctx.cwd).context(activeFocus.id, { view: "review" }) as any;
    const text = assistantText(event.message);
    if (projected?.markdown && text.includes(projected.markdown.trim())) await domain(ctx.cwd).markReviewPresented(activeFocus.id, focus.activeReview.id);
  });

  pi.on("agent_settled", async () => { currentInteractionRef = undefined; });

  return {
    isActive: () => modeActive,
    getActiveFocus: (ctx: CommandContext) => ({ id: requireFocus(ctx), repositoryRoot: resolve(ctx.cwd) }),
    suspend: (ctx: CommandContext) => {
      if (activeFocus && activeFocus.repositoryRoot === resolve(ctx.cwd)) appendLink(ctx.cwd, activeFocus.id, "suspended");
      deactivate();
    },
    handleMigrateCommand: async (args: string, ctx: CommandContext) => {
      if (args.trim()) {
        ctx.ui.notify("Usage: /dag migrate", "error");
        return true;
      }
      currentInteractionRef = undefined;
      if (activeFocus && activeFocus.repositoryRoot === resolve(ctx.cwd)) appendLink(ctx.cwd, activeFocus.id, "suspended");
      deactivate();
      const result = await bootstrapProjectMigration(ctx.cwd);
      activate(ctx.cwd, result.focusId, "migration");
      appendLink(ctx.cwd, result.focusId, "active");
      pi.sendMessage({
        customType: KICKOFF_MESSAGE,
        content: `Enter project-model migration mode for ${result.focusId}. Candidate ${result.created ? "created" : "resumed"}; legacy adapter ${result.usedLegacyAdapter ? "used" : "not used"}; inventoried ${result.sourceCount} sources and ${result.artifactCount} existing spec artifacts. Candidate manifest: ${result.candidateManifestHash}. Inspect the migration metadata and repository, build the semantic candidate, then present the required Lavish audit and exact cutover/coexistence choice.`,
        display: true,
      }, { triggerTurn: true, deliverAs: "followUp" });
      return true;
    },
    handleBrainstormCommand: async (args: string, ctx: CommandContext) => {
      // The command title/seed is routing input, not semantic authority.
      currentInteractionRef = undefined;
      const parsed = parseBrainstormArgs(args);
      const projectDomain = domain(ctx.cwd);
      if (!(await projectDomain.models.exists())) {
        ctx.ui.notify(`Project model not found at ${DEFAULT_PROJECT_MODEL_PATH}. Generate or migrate a candidate before entering model brainstorming.`, "error");
        return true;
      }
      if (parsed.action === "list") {
        const sessions = await projectDomain.sessions.list();
        ctx.ui.notify(sessions.length ? sessions.map(({ id, title, status }) => `${id} — ${title} (${status})`).join("\n") : "No model focus sessions.", "info");
        return true;
      }
      if (parsed.action === "stop") {
        if (activeFocus && activeFocus.repositoryRoot === resolve(ctx.cwd)) await projectDomain.sessions.mutate(activeFocus.id, (session) => { session.status = "suspended"; });
        if (activeFocus && activeFocus.repositoryRoot === resolve(ctx.cwd)) appendLink(ctx.cwd, activeFocus.id, "suspended");
        deactivate();
        ctx.ui.notify("Model brainstorming suspended.", "info");
        return true;
      }
      let focus;
      if (parsed.action === "resume") {
        if (!parsed.id) { ctx.ui.notify("Usage: /dag brainstorm resume <focus-id>", "error"); return true; }
        focus = await projectDomain.sessions.load(parsed.id);
        focus = await projectDomain.sessions.mutate(focus.id, (session) => { session.status = "active"; });
      } else if (parsed.action === "new") {
        const title = parsed.title || (ctx.hasUI ? await ctx.ui.input("Focus session title") : undefined);
        if (!title) { ctx.ui.notify("Usage: /dag brainstorm new <name>", "error"); return true; }
        const available = (await projectDomain.models.load()).workstreams.map(({ id }) => id);
        const workstreamIds = parsed.workstreamIds ?? available;
        for (const id of workstreamIds) if (!available.includes(id)) throw new Error(`Unknown workstream: ${id}`);
        focus = await projectDomain.sessions.create({ title, seed: parsed.seed || title, workstreamIds });
      } else {
        const sessions = await projectDomain.sessions.list();
        if (!ctx.hasUI) {
          ctx.ui.notify(`Headless /dag brainstorm requires new <name> or resume <id>.${sessions.length ? ` Available: ${sessions.map(({ id }) => id).join(", ")}.` : ""}`, "error");
          return true;
        }
        const action = await ctx.ui.select("Model brainstorming", sessions.length ? ["Resume", "New"] : ["New"]);
        if (!action) return true;
        if (action === "Resume") {
          const id = sessions.length === 1 ? sessions[0].id : await ctx.ui.select("Resume which focus?", sessions.map(({ id, title }) => `${id} — ${title}`));
          if (!id) return true;
          const selectedId = id.split(" — ")[0];
          focus = await projectDomain.sessions.load(selectedId);
          focus = await projectDomain.sessions.mutate(focus.id, (session) => { session.status = "active"; });
        } else {
          const title = await ctx.ui.input("Focus session title");
          if (!title) return true;
          const workstreamIds = (await projectDomain.models.load()).workstreams.map(({ id }) => id);
          focus = await projectDomain.sessions.create({ title, seed: title, workstreamIds });
        }
      }
      activate(ctx.cwd, focus.id);
      appendLink(ctx.cwd, focus.id, "active");
      const orientation = await projectDomain.context(focus.id, { view: "orientation" });
      pi.sendMessage({
        customType: KICKOFF_MESSAGE,
        content: `Enter model brainstorming focus ${focus.id}. User seed: ${focus.seed ?? "(none)"}. Orientation: ${JSON.stringify(orientation)}. Continue the mixed-initiative loop; classify the original seed normally and use model tools for semantic state.`,
        display: true,
      }, { triggerTurn: true, deliverAs: "followUp" });
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

export function lavishFeedbackInteractionRef(reviewId: string, reviewHash: string, feedback: LavishFeedback): string {
  return `lavish-feedback:${reviewId}:${sha256({ reviewHash, prompts: feedback.prompts })}`;
}

function assistantText(message: any): string {
  if (typeof message?.content === "string") return message.content;
  if (!Array.isArray(message?.content)) return "";
  return message.content.filter((part: any) => part?.type === "text" && typeof part.text === "string").map((part: any) => part.text).join("\n");
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
    focusId: value.focusId,
    reviewId: value.reviewId,
    artifactPath: value.artifactPath,
    status: value.status,
    promptCount: value.feedback?.prompts?.length,
    feedbackTruncated: value.feedback?.truncation?.truncated,
  };
}

function latestFocusLink(ctx: any, repositoryRoot: string): LinkEntry | undefined {
  const entries = ctx.sessionManager.getBranch?.() ?? ctx.sessionManager.getEntries?.() ?? [];
  for (let index = entries.length - 1; index >= 0; index--) {
    const entry = entries[index];
    if (entry?.type === "custom" && entry.customType === LINK_ENTRY && entry.data?.repositoryRoot === repositoryRoot) return entry.data as LinkEntry;
  }
  return undefined;
}

function parseBrainstormArgs(input: string): { action: "select" | "new" | "resume" | "list" | "stop"; id?: string; title?: string; seed?: string; workstreamIds?: string[] } {
  const values = splitArgs(input);
  const workstreamArg = values.find((value) => value.startsWith("--workstreams="));
  const workstreamIds = workstreamArg ? workstreamArg.slice("--workstreams=".length).split(",").map((id) => id.trim()).filter(Boolean) : undefined;
  if (workstreamArg) values.splice(values.indexOf(workstreamArg), 1);
  const action = values.shift();
  if (!action) return { action: "select" };
  if (action === "list" || action === "stop") return { action };
  if (action === "resume") return { action, id: values[0] ? normalizeFocusId(values[0]) : undefined };
  if (action === "new") {
    const title = values.join(" ").trim();
    return { action, title: title || undefined, seed: title || undefined, workstreamIds };
  }
  return { action: "new", title: [action, ...values].join(" "), seed: [action, ...values].join(" ") };
}

function splitArgs(input: string): string[] {
  const values: string[] = [];
  const pattern = /"([^"]*)"|'([^']*)'|([^\s]+)/g;
  let match;
  while ((match = pattern.exec(input))) values.push(match[1] ?? match[2] ?? match[3]);
  return values;
}

const MODEL_MODE_GUIDANCE = `You are in mixed-initiative model brainstorming mode.
- Read narrow model context before acting; research repository/external evidence before asking.
- Write coherent sourced discoveries, evidence, assumptions, questions, tensions, scenarios, and proposals immediately with dag_model_update; never use it to grant authority.
- Use dag_model_record_direction only for unambiguous direct user direction from the current interaction. Agent-derived implications remain non-authoritative.
- Initiate broad review turns only on material triggers. Separate For awareness from exact Decisions needed. Use only real alternatives.
- Write question briefs as scannable Markdown. Establish the decision, why it matters, the recommendation, and the exact question; adaptively add current behavior, change context, constraints, consequences, uncertainty, and model evidence when they support an informed choice.
- Keep Current understanding causal, current, explicitly non-authoritative, and grounded in exact object refs. Use adaptive Markdown to expose the goal, relevant current state or mechanism, and governing direction, adding other sections only when useful.
- Demonstrate comprehension through selective causal synthesis rather than reproducing the project model or filling an exhaustive template. Do not require a distinct formal acknowledgement surface.
- Resolve sparse responses independently through dag_model_resolve_review; stale, omitted, ambiguous, or conflicting points remain open. After a frontier review resolves, automatically explore and present the next supported material frontier until none remains, the user redirects, or unresolved user input blocks progress; never invent questions to claim exhaustion.
- New prototypes require explicit user request. Planning, chunking, DAG execution, and archive are unavailable in model mode; use the optional presentation adapter for Lavish review when requested.`;

const MIGRATION_MODE_GUIDANCE = `You are in guided project-model migration mode for an existing repository.
- Start with dag_model_context orientation. Inspect relevant repository evidence in tiers: supported legacy state; repository/package orientation; README, specs, docs, ADRs, and plans; tests or representative code when they confirm or contradict behavior; bounded Git history only for material ambiguity.
- Build a coherent non-authoritative candidate with dag_model_update. Agents classify meaning; tools own IDs, hashes, validation, and persistence. Never copy every file or mistake implementation detail for governing product direction.
- Keep project.migration current through dag_model_update migration. Classify every inventoried source as mapped, retained, or omitted with rationale. Classify every relevant artifact as create_generated, replace_generated, retain_reference, retain_evidence, or block. The tool records exact observed and generated hashes.
- A partial candidate may be presented, but set migration phase ready only when the model and projections are coherent, every material source and artifact is dispositioned, authority conflicts are resolved, and blockers are empty. Never report a synthetic completeness percentage.
- Before cutover, create one review with an awareness summary and a decision asking whether to cut over or continue refining/coexisting. Its options may omit semantic direction payloads because cutover is an isolated authority operation. Present it with dag_model_present_review and rich blocks that show inferred goals/direction, workstreams and counts, unresolved questions, source mappings/omissions, generated-spec diffs, blockers, and an artifact-disposition table.
- Treat Lavish feedback as the exact human interaction. If the user chooses cutover, call dag_model_record_direction with only cutover.candidateManifestHash from the current candidate. If the user asks for side-by-side artifacts or changes, resolve that directionless migration option with dag_model_resolve_review, update dispositions/model meaning, and present a refreshed review instead. Never cut over while migration readiness or file freshness fails.
- Physical coexistence is allowed; dual semantic authority is not. Retained specs remain linked references or evidence. If an existing artifact must remain governing, keep a blocker and do not cut over.
- Planning and DAG execution remain unavailable until migration cutover completes.`;
