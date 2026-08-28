import { createHash, randomUUID } from "node:crypto";
import { execFile } from "node:child_process";
import { lstat, readFile, realpath } from "node:fs/promises";
import { relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import type { DagConductorServiceV1, DagSessionRunBindingV1 } from "../dag-runtime/conductor.ts";
import { allObjects, assertValidProjectModel, semanticHash } from "../project-model/model.ts";
import { ProjectModelDomain } from "../project-model/domain.ts";
import type { ProjectModel } from "../project-model/types.ts";
import { createDagPlanningPlanV1 } from "./artifact.ts";
import {
  projectDagPlanningLineageV1,
  projectDagPlanningNodeV1,
  renderDagPlanningGraphV1,
  renderDagPlanningMarkdownV1,
} from "./projections.ts";
import { prepareDagRunV1 } from "./runtime-adapter.ts";
import { DagPlanningStoreV1 } from "./store.ts";
import type { DagPlanningPlanInputV1, DagPlanningPlanV1, DagPlanningSourceRefV1 } from "./types.ts";

const run = promisify(execFile);
const PLAN_MESSAGE = "dag-planning-request-v1";
const PLAN_BINDING_ENTRY = "dag-planning-session-binding-v1";
const MODEL_PATH = "project-model/model.json";
const TOOL_COLLECTIONS = [
  "intents", "concepts", "scenarios", "decisions", "commitments",
  "evidence", "questions", "proposals", "discoveries",
] as const;
const RequiredText = Type.String({ minLength: 1, maxLength: 65_536 });
const ShortText = Type.String({ minLength: 1, maxLength: 512 });
const Id = Type.String({ minLength: 1, maxLength: 128, pattern: "^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$" });
const TextList = Type.Array(RequiredText, { maxItems: 512 });
const Collection = Type.Union(TOOL_COLLECTIONS.map((value) => Type.Literal(value)));
const RequestedSource = Type.Union([
  Type.Object({ kind: Type.Literal("project_model_object"), collection: Collection, objectId: Type.String({ minLength: 1, maxLength: 512 }), summary: Type.Optional(ShortText) }),
  Type.Object({ kind: Type.Literal("generated_spec"), path: Type.String({ minLength: 1, maxLength: 4_096 }), summary: Type.Optional(ShortText) }),
]);
const Outcome = Type.Object({ id: Id, description: RequiredText });
const ValidationCommand = Type.Object({
  id: Id,
  argv: Type.Array(Type.String({ minLength: 1, maxLength: 1_024 }), { minItems: 1, maxItems: 32 }),
});
const WorkItem = Type.Object({
  id: Id,
  title: ShortText,
  objective: RequiredText,
  outcomeIds: Type.Array(Id, { minItems: 1, maxItems: 256 }),
  context: TextList,
  checks: Type.Array(RequiredText, { minItems: 1, maxItems: 256 }),
  dependsOn: Type.Array(Id, { maxItems: 256 }),
  risk: Type.Union([Type.Literal("low"), Type.Literal("medium"), Type.Literal("high")]),
  riskNotes: TextList,
  constraints: Type.Optional(TextList),
});
const SAVE_PARAMETERS = Type.Object({
  planId: Id,
  expectedRevision: Type.Optional(Type.Integer({ minimum: 1 })),
  title: ShortText,
  source: Type.Object({ refs: Type.Array(RequestedSource, { minItems: 1, maxItems: 256 }), scopeSummary: RequiredText }),
  architecture: Type.Object({ outcomes: Type.Array(Outcome, { minItems: 1, maxItems: 256 }), nonGoals: TextList, notes: TextList, risks: TextList }),
  workItems: Type.Array(WorkItem, { minItems: 1, maxItems: 1_024 }),
  constraints: Type.Object({
    maxConcurrency: Type.Union([Type.Integer({ minimum: 1 }), Type.Null()]),
    mutexGroups: Type.Array(Type.Object({ id: Id, workItemIds: Type.Array(Id, { minItems: 2, maxItems: 256 }), reason: RequiredText }), { maxItems: 256 }),
  }),
  integration: Type.Object({
    strategy: Type.Union([Type.Literal("dependency_order"), Type.Literal("serial")]),
    checks: Type.Array(RequiredText, { minItems: 1, maxItems: 256 }),
    finalChecks: Type.Array(RequiredText, { minItems: 1, maxItems: 256 }),
    prefixCommands: Type.Array(ValidationCommand, { minItems: 1, maxItems: 16 }),
    finalCommands: Type.Array(ValidationCommand, { minItems: 1, maxItems: 16 }),
  }),
});
const DECIDE_PARAMETERS = Type.Object({
  planId: Id,
  expectedRevision: Type.Integer({ minimum: 1 }),
  approve: Type.Optional(Type.Literal(true)),
  approvalNote: Type.Optional(RequiredText),
  authorize: Type.Optional(Type.Object({
    scope: Type.Array(RequiredText, { minItems: 1, maxItems: 512 }),
    maxConcurrency: Type.Integer({ minimum: 1 }),
    note: Type.Optional(RequiredText),
  })),
});

interface ActiveFocus {
  id: string;
  repositoryRoot: string;
  orientation?: unknown;
}

interface IntegrationOptions {
  getActiveFocus(ctx: any): ActiveFocus | Promise<ActiveFocus>;
  conductor: Pick<DagConductorServiceV1, "binding" | "status" | "inspect" | "startPrepared" | "startIdentity"> & Partial<Pick<DagConductorServiceV1, "pendingStart">>;
}

interface ParsedCommandInput {
  rest: string;
  options: Record<string, string | boolean>;
}

interface PlanningSessionBindingV1 {
  repositoryRoot: string;
  sessionId: string;
  planId: string;
  revision: number;
  planHash: string;
}

/** Product-facing plan/show/run integration over the strict thin-plan store. */
export function registerDagPlanningIntegrationV1(pi: ExtensionAPI, options: IntegrationOptions) {
  const stores = new Map<string, DagPlanningStoreV1>();
  const storeFor = (root: string) => {
    const key = resolve(root);
    let store = stores.get(key);
    if (!store) { store = new DagPlanningStoreV1(key); stores.set(key, store); }
    return store;
  };

  registerTool(pi, {
    name: "dag_plan_save",
    label: "Save DAG Plan",
    description: "Create or revise the strict thin plan for the exact active focus. Repository and current source identities are derived and checked internally; returns deterministic Markdown and graph previews.",
    parameters: SAVE_PARAMETERS,
    execute: async (params: any, ctx: any) => {
      const focus = await requireFocus(options.getActiveFocus, ctx);
      const git = await exactCleanGit(ctx.cwd, focus.repositoryRoot);
      const resolved = await resolveRequestedSources(git.root, git.commit, params.source.refs);
      const staticInput = {
        status: "draft" as const,
        title: params.title,
        focusId: focus.id,
        repository: { repositoryId: git.repositoryId, baselineCommit: git.commit, baselineTree: git.tree, targetBranch: git.branchRef },
        source: { refs: resolved.refs, scopeSummary: params.source.scopeSummary },
        architecture: structuredClone(params.architecture),
        workItems: structuredClone(params.workItems),
        constraints: structuredClone(params.constraints),
        integration: structuredClone(params.integration),
      };
      const store = storeFor(git.root);
      let plan: DagPlanningPlanV1;
      if (await store.exists(params.planId)) {
        if (!Number.isSafeInteger(params.expectedRevision)) throw new Error("Revising a DAG plan requires its exact expected revision");
        const current = await store.read(params.planId);
        assertPlanFocus(current, focus);
        if (current.repository.repositoryId !== git.repositoryId) throw new Error("DAG plan repository identity does not match the active repository");
        const changed = await store.mutateDraft(params.planId, params.expectedRevision, (draft) => {
          Object.assign(draft, structuredClone(staticInput));
        });
        plan = changed.plan;
      } else {
        if (params.expectedRevision !== undefined) throw new Error("A new DAG plan cannot declare an expected revision");
        plan = await store.create(createDagPlanningPlanV1({
          planId: params.planId,
          ...staticInput,
          approval: { status: "pending", by: null, at: null, note: null },
          authorization: { status: "not_authorized", by: null, at: null, scope: [], maxConcurrency: null, note: null },
        } satisfies DagPlanningPlanInputV1));
      }
      bindPlan(pi, ctx, git.root, plan);
      const markdown = renderDagPlanningMarkdownV1(plan);
      const graph = renderDagPlanningGraphV1(plan);
      return {
        content: [{ type: "text", text: `${markdown}\n---\n\n${graph}` }],
        details: { action: "save", planId: plan.planId, revision: plan.revision, planHash: plan.planHash, focusId: plan.focusId, markdown, graph },
      };
    },
  });

  registerTool(pi, {
    name: "dag_plan_decide",
    label: "Decide DAG Plan",
    description: "Record an explicit approval and, independently and optionally, a bounded run authorization for one exact current plan revision. This never starts execution.",
    parameters: DECIDE_PARAMETERS,
    execute: async (params: any, ctx: any) => {
      if (params.approve !== true && !params.authorize) throw new Error("A plan decision must explicitly approve or authorize");
      const focus = await requireFocus(options.getActiveFocus, ctx);
      const root = await exactRepositoryRoot(ctx.cwd, focus.repositoryRoot);
      const plan = await decidePlan(storeFor(root), params.planId, params.expectedRevision, {
        approve: params.approve === true,
        approvalNote: params.approvalNote,
        authorize: params.authorize,
        actor: sessionActor(ctx),
      });
      bindPlan(pi, ctx, root, plan);
      return {
        content: [{ type: "text", text: decisionSummary(plan) }],
        details: { action: "decide", planId: plan.planId, revision: plan.revision, planHash: plan.planHash, approval: plan.approval.status, authorization: plan.authorization.status, executionStarted: false },
      };
    },
  });

  async function handleCommand(command: string, restOrParsed: string | ParsedCommandInput = "", optionsOrCtx: Record<string, string | boolean> | any = {}, maybeCtx?: any): Promise<boolean> {
    const { rest, commandOptions, ctx } = normalizeHandleArguments(restOrParsed, optionsOrCtx, maybeCtx);
    if (!["plan", "show", "run"].includes(command)) return false;
    try {
      if (command === "plan") await handlePlanCommand(rest, commandOptions, ctx);
      else if (command === "show") await handleShowCommand(rest, commandOptions, ctx);
      else await handleRunCommand(rest, commandOptions, ctx);
    } catch (error: any) {
      ctx.ui.notify(error?.message ?? String(error), "error");
    }
    return true;
  }

  async function handlePlanCommand(rest: string, commandOptions: Record<string, string | boolean>, ctx: any): Promise<void> {
    const tokens = splitArgs(rest);
    const subcommand = tokens[0];
    if (subcommand === "approve" || subcommand === "authorize") {
      if (tokens.length !== 1) throw new Error(`Usage: /dag plan ${subcommand} --plan planId@revision`);
      const selector = exactRevisionOption(commandOptions.plan);
      const focus = await requireFocus(options.getActiveFocus, ctx);
      const root = await exactRepositoryRoot(ctx.cwd, focus.repositoryRoot);
      const store = storeFor(root);
      const selected = await store.select(selector);
      assertPlanFocus(selected, focus);
      await assertSelectedHead(store, selected);
      const authorization = subcommand === "authorize" ? {
        scope: selected.workItems.map(({ id }) => id).sort(),
        maxConcurrency: selected.constraints.maxConcurrency ?? selected.workItems.length,
        note: "Explicit /dag plan authorize command.",
      } : undefined;
      const plan = await decidePlan(store, selected.planId, selected.revision, {
        approve: subcommand === "approve",
        approvalNote: subcommand === "approve" ? "Explicit /dag plan approve command." : undefined,
        authorize: authorization,
        actor: sessionActor(ctx),
      });
      bindPlan(pi, ctx, root, plan);
      ctx.ui.notify(decisionSummary(plan), "info");
      return;
    }

    if (commandOptions.new && commandOptions.plan) throw new Error("/dag plan accepts either --new or --plan, not both");
    // The host's generic parser may consume the first goal word as --new's value.
    // Recover it as positional goal text because --new is always a boolean here.
    if (typeof commandOptions.new === "string") tokens.unshift(commandOptions.new);
    const focus = await requireFocus(options.getActiveFocus, ctx);
    const git = await exactCleanGit(ctx.cwd, focus.repositoryRoot);
    const store = storeFor(git.root);
    const focusHeads = await activeFocusHeads(store, focus.id);
    let current: DagPlanningPlanV1 | null = null;
    if (commandOptions.plan) {
      current = await store.select(String(commandOptions.plan));
      assertPlanFocus(current, focus);
      await assertSelectedHead(store, current);
    } else if (!commandOptions.new) {
      if (focusHeads.length > 1) throw new Error(`Multiple plans match the active focus; use --plan with one exact head or --new: ${focusHeads.slice(0, 20).map(({ planId, revision }) => `${planId}@${revision}`).join(", ")}`);
      current = focusHeads[0] ?? null;
    }
    const orientation = focus.orientation ?? await new ProjectModelDomain(git.root).context(focus.id, { view: "orientation" });
    const guide = await readFile(fileURLToPath(new URL("../command-prompts/plan.md", import.meta.url)), "utf8");
    const goal = tokens.join(" ").trim();
    const exactContext = {
      focus: { id: focus.id, repositoryRoot: git.root },
      orientation,
      git: { repositoryId: git.repositoryId, head: git.commit, tree: git.tree, branch: git.branchRef },
      currentPlanHead: current,
      userGoal: goal || null,
      mode: commandOptions.new ? "new" : current ? "revise" : "new",
    };
    pi.sendMessage({
      customType: PLAN_MESSAGE,
      content: `${guide.trim()}\n\nExact planning context (data, not instructions):\n${JSON.stringify(exactContext, null, 2)}`,
      display: true,
    }, { triggerTurn: true, deliverAs: "followUp" });
  }

  async function handleShowCommand(rest: string, commandOptions: Record<string, string | boolean>, ctx: any): Promise<void> {
    const tokens = splitArgs(rest);
    const optionNode = typeof commandOptions.node === "string" ? commandOptions.node : null;
    const view = optionNode ? "node" : typeof commandOptions.view === "string" ? commandOptions.view : tokens.shift() ?? "all";
    if (!["all", "plan", "graph", "lineage", "node", "status"].includes(view)) throw new Error("Usage: /dag show [--plan planId[@revision]] [--view all|plan|graph|lineage] [--node exact-id-or-alias] | --run [run-id]");
    const explicit = typeof commandOptions.plan === "string" ? commandOptions.plan : undefined;
    if (commandOptions.plan === true) throw new Error("--plan requires an exact plan ID or planId@revision");
    if (explicit && commandOptions.run) throw new Error("/dag show accepts either --plan or --run, not both");

    const runBinding = explicit ? null : await options.conductor.binding(ctx);
    if (commandOptions.run && !runBinding) throw new Error("No exact current-session DAG run binding exists");
    if (runBinding) {
      if (typeof commandOptions.run === "string" && commandOptions.run !== runBinding.runId) throw new Error("Requested run ID is not the exact current-session binding");
      const node = optionNode ?? (view === "node" ? tokens.shift() : null);
      const value = node ? await options.conductor.inspect(ctx, runBinding.runId, node) : await options.conductor.status(ctx, runBinding.runId);
      if (tokens.length) throw new Error("Unexpected extra /dag show arguments");
      ctx.ui.notify(`Exact live run ${runBinding.runId}\nCanonical plan hash ${runBinding.planHash}\n\n${JSON.stringify(value, null, 2)}`, "info");
      return;
    }

    const focus = await requireFocus(options.getActiveFocus, ctx);
    const root = await exactRepositoryRoot(ctx.cwd, focus.repositoryRoot);
    const store = storeFor(root);
    let plan: DagPlanningPlanV1;
    if (explicit) {
      plan = await store.select(explicit);
      assertPlanFocus(plan, focus);
    } else {
      const sessionBinding = latestPlanBinding(ctx, root);
      if (sessionBinding) plan = await readBoundPlan(store, sessionBinding);
      else {
        const heads = await activeFocusHeads(store, focus.id);
        if (heads.length !== 1) throw new Error(heads.length ? `Multiple plans match the active focus; use --plan: ${heads.slice(0, 20).map(({ planId, revision }) => `${planId}@${revision}`).join(", ")}` : "No plan matches the active focus; use /dag plan first");
        plan = heads[0];
      }
    }
    const header = `Exact static plan ${plan.planId}@${plan.revision}\nStatic plan hash ${plan.planHash}`;
    let body: string;
    if (view === "plan") body = renderDagPlanningMarkdownV1(plan);
    else if (view === "graph") body = renderDagPlanningGraphV1(plan);
    else if (view === "lineage") body = JSON.stringify(projectDagPlanningLineageV1(await store.listRevisions(plan.planId)), null, 2);
    else if (view === "node") {
      const node = optionNode ?? tokens.shift();
      if (!node || tokens.length) throw new Error("Usage: /dag show --node <exact-node-id-or-alias> [--plan planId[@revision]]");
      body = JSON.stringify(projectDagPlanningNodeV1(plan, node), null, 2);
    } else {
      if (view === "status") throw new Error("The status view requires an exact current-session live run");
      if (tokens.length) throw new Error("Unexpected extra /dag show arguments");
      body = [renderDagPlanningMarkdownV1(plan), renderDagPlanningGraphV1(plan), JSON.stringify(projectDagPlanningLineageV1(await store.listRevisions(plan.planId)), null, 2)].join("\n---\n\n");
    }
    ctx.ui.notify(`${header}\n\n${body}`.trimEnd(), "info");
  }

  async function handleRunCommand(rest: string, commandOptions: Record<string, string | boolean>, ctx: any): Promise<void> {
    if (rest.trim()) throw new Error("Usage: /dag run [--plan planId@revision] | /dag run --resume");
    if (commandOptions.resume && commandOptions.plan) throw new Error("/dag run accepts either --resume or --plan, not both");
    const binding = await options.conductor.binding(ctx);
    if (binding) {
      if (commandOptions.plan === true) throw new Error("--plan requires an exact plan ID or planId@revision");
      if (typeof commandOptions.plan === "string") {
        const focus = await requireFocus(options.getActiveFocus, ctx);
        const root = await exactRepositoryRoot(ctx.cwd, focus.repositoryRoot);
        const selected = await storeFor(root).select(commandOptions.plan);
        assertPlanFocus(selected, focus);
        await assertSelectedHead(storeFor(root), selected);
        const source = await options.conductor.startIdentity(ctx, binding.runId);
        if (source.sourcePlanningPlanId !== selected.planId || source.sourcePlanningPlanHash !== selected.planHash) throw new Error("Explicit plan selector does not match the exact current-session run source");
      }
      const live = await options.conductor.status(ctx, binding.runId);
      ctx.ui.notify(runSummary(binding, live.state, live.projection), "info");
      sendRunOrchestrationKickoff(binding.runId);
      return;
    }
    if (commandOptions.resume) throw new Error("No exact current-session DAG run binding exists to resume");
    if (commandOptions.plan === true) throw new Error("--plan requires an exact plan ID or planId@revision");
    const focus = await requireFocus(options.getActiveFocus, ctx);
    const root = await exactRepositoryRoot(ctx.cwd, focus.repositoryRoot);
    const store = storeFor(root);
    const eligible = await eligibleFocusHeads(store, focus.id);
    let selected: DagPlanningPlanV1;
    if (typeof commandOptions.plan === "string") {
      selected = await store.select(commandOptions.plan);
      assertPlanFocus(selected, focus);
      await assertSelectedHead(store, selected);
      if (!isRunEligible(selected)) throw new Error(`DAG plan ${selected.planId}@${selected.revision} is not ready, approved, and authorized`);
    } else {
      if (eligible.length === 0) throw new Error("No active-focus plan head is ready, approved, and authorized");
      if (eligible.length > 1) throw new Error(`Multiple active-focus plan heads are runnable; use --plan with one exact selector: ${eligible.slice(0, 20).map(({ planId, revision }) => `${planId}@${revision}`).join(", ")}`);
      selected = eligible[0];
    }
    const pending = await options.conductor.pendingStart?.(ctx, selected.planId, selected.planHash) ?? null;
    const occurredAt = pending?.startedAt ?? new Date().toISOString();
    const runId = pending?.runId ?? `run-${randomUUID()}`;
    const runNonce = pending?.runNonce ?? `${randomUUID()}-${randomUUID()}`;
    const prepared = await prepareDagRunV1({ planningPlan: selected, repositoryRoot: root, runId, runNonce, createdAt: occurredAt });
    if (pending && prepared.canonicalPlan.planHash !== pending.planHash) throw new Error("Unfinished DAG start no longer compiles to its exact canonical plan identity");
    const started = await options.conductor.startPrepared(ctx, {
      runId,
      runNonce,
      planHash: prepared.canonicalPlan.planHash,
      maxActiveNodes: prepared.genesis.scheduler.maxActiveNodes,
      occurredAt,
      plan: prepared.canonicalPlan,
      genesis: prepared.genesis,
      context: prepared.context,
      seedFacts: [...prepared.seedFacts],
      sourcePlanningPlanId: selected.planId,
      sourcePlanningPlanHash: selected.planHash,
    });
    bindPlan(pi, ctx, root, selected);
    ctx.ui.notify(runSummary(started.binding, started.state, null), "info");
    sendRunOrchestrationKickoff(started.state.runId);
  }

  function sendRunOrchestrationKickoff(runId: string): void {
    pi.sendMessage({
      customType: "dag-run-orchestration-kickoff",
      content: `Orchestrate canonical DAG ${runId}. Call dag_next_action now and choose one current semantic action. Use only dag_start_work, dag_run_checks, dag_record_completion, dag_integrate, dag_retry, dag_pause, dag_resume, dag_cancel, and dag_finalize; these tools derive all internal guards. Never use generic subagent for canonical DAG work. After every mutation, refresh dag_next_action because all prior choices were revision-bound. End the turn at an owned-worker dependency barrier. Completion callbacks wake you with exact pre-bind recovery or recording guidance; notifications never mutate canonical DAG state and have no arbitrary timeout.`,
      display: true,
    }, { triggerTurn: true, deliverAs: "followUp" });
  }

  return { handleCommand };
}

async function decidePlan(
  store: DagPlanningStoreV1,
  planId: string,
  expectedRevision: number,
  input: { approve: boolean; approvalNote?: string; authorize?: { scope: string[]; maxConcurrency: number; note?: string }; actor: string },
): Promise<DagPlanningPlanV1> {
  let current = await store.read(planId);
  if (current.revision !== expectedRevision) throw new Error(`DAG planning record ${planId} revision conflict: expected ${expectedRevision}, found ${current.revision}`);
  const staticHash = current.planHash;
  if (input.approve) {
    const now = new Date().toISOString();
    const result = await store.mutateDecision(planId, current.revision, (decision) => {
      if (decision.approval.status !== "pending") throw new Error("Plan approval has already been decided");
      decision.status = "ready";
      decision.approval = { status: "approved", by: input.actor, at: now, note: input.approvalNote ?? null };
    }, now);
    if (result.beforeHash !== staticHash || result.plan.planHash !== staticHash) throw new Error("Plan changed while recording approval");
    current = result.plan;
  }
  if (input.authorize) {
    const now = new Date().toISOString();
    const result = await store.mutateDecision(planId, current.revision, (decision) => {
      if (decision.status !== "ready" || decision.approval.status !== "approved") throw new Error("Authorization requires a separately retained approved ready-plan revision");
      if (decision.authorization.status !== "not_authorized") throw new Error("Plan authorization has already been decided");
      decision.authorization = {
        status: "authorized",
        by: input.actor,
        at: now,
        scope: [...input.authorize!.scope],
        maxConcurrency: input.authorize!.maxConcurrency,
        note: input.authorize!.note ?? null,
      };
    }, now);
    if (result.beforeHash !== staticHash || result.plan.planHash !== staticHash) throw new Error("Plan changed while recording authorization");
    current = result.plan;
  }
  return current;
}

async function resolveRequestedSources(root: string, head: string, requested: any[]): Promise<{ refs: DagPlanningSourceRefV1[]; model: ProjectModel }> {
  const modelBytes = await trackedHeadBytes(root, head, MODEL_PATH);
  const model = JSON.parse(modelBytes.toString("utf8")) as ProjectModel;
  assertValidProjectModel(model);
  if (model.project.mode !== "authoritative") throw new Error("DAG planning requires authoritative project-model/model.json");
  const governingCollections = new Set(["intents", "concepts", "scenarios", "decisions", "commitments"]);
  const objects = new Map(allObjects(model).map((entry) => [`${entry.collection}\0${entry.object.id}`, entry]));
  const specsByPath = new Map<string, typeof model.project.projections.specs>();
  for (const spec of model.project.projections.specs) specsByPath.set(spec.path, [...(specsByPath.get(spec.path) ?? []), spec]);
  const refs: DagPlanningSourceRefV1[] = [];
  let governingCount = 0;
  for (const source of requested) {
    if (source.kind === "project_model_object") {
      const entry = objects.get(`${source.collection}\0${source.objectId}`);
      if (!entry) throw new Error(`Requested project-model object does not resolve exactly: ${source.collection}/${source.objectId}`);
      const exactSemanticHash = semanticHash(entry.collection, entry.object);
      if (governingCollections.has(entry.collection)) {
        if (entry.object.state !== "accepted" || entry.object.acceptance?.contentHash !== exactSemanticHash) throw new Error(`Governing planning source is not exact accepted authority: ${source.collection}/${source.objectId}`);
        governingCount += 1;
      }
      refs.push({ kind: "project_model_object", collection: source.collection, objectId: source.objectId, semanticHash: exactSemanticHash, ...(source.summary ? { summary: source.summary } : {}) });
      continue;
    }
    const projections = specsByPath.get(source.path) ?? [];
    if (projections.length !== 1) throw new Error(`Requested generated specification does not resolve exactly: ${source.path}`);
    const bytes = await trackedHeadBytes(root, head, source.path);
    refs.push({ kind: "generated_spec", path: source.path, contentHash: hashBytes(bytes), ...(source.summary ? { summary: source.summary } : {}) });
  }
  if (governingCount === 0) throw new Error("A runnable DAG plan requires at least one exact accepted governing model source");
  return { refs, model };
}

async function trackedHeadBytes(root: string, head: string, path: string): Promise<Buffer> {
  assertSafeRepositoryPath(root, path);
  await git(root, ["ls-files", "--error-unmatch", "--", path]);
  const absolute = resolve(root, path);
  const metadata = await lstat(absolute);
  if (!metadata.isFile() || metadata.isSymbolicLink()) throw new Error(`Planning source must be a regular tracked file: ${path}`);
  const canonical = await realpath(absolute);
  if (canonical !== root && !canonical.startsWith(`${root}${sep}`)) throw new Error(`Planning source escapes the repository: ${path}`);
  const working = await readFile(canonical);
  const committed = await gitBuffer(root, ["show", `${head}:${path}`]);
  if (!working.equals(committed)) throw new Error(`Planning source bytes differ from current HEAD: ${path}`);
  return working;
}

async function exactCleanGit(cwd: string, expectedRoot: string) {
  const root = await exactRepositoryRoot(cwd, expectedRoot);
  const status = await git(root, ["status", "--porcelain=v2", "--untracked-files=all"]);
  if (status) throw new Error("Repository must be clean before saving or revising a DAG plan");
  const commit = await git(root, ["rev-parse", "HEAD^{commit}"]);
  const tree = await git(root, ["rev-parse", "HEAD^{tree}"]);
  const branchRef = await git(root, ["symbolic-ref", "-q", "HEAD"]);
  if (!branchRef.startsWith("refs/heads/")) throw new Error("DAG planning requires a checked-out local branch");
  const commonRaw = await git(root, ["rev-parse", "--path-format=absolute", "--git-common-dir"]);
  const common = await realpath(commonRaw);
  const objectFormat = await git(root, ["rev-parse", "--show-object-format"]);
  const repositoryId = `repo-${createHash("sha256").update(`${common}\0${objectFormat}`).digest("hex").slice(0, 32)}`;
  return { root, commit, tree, branchRef, repositoryId };
}

async function exactRepositoryRoot(cwd: string, expectedRoot: string): Promise<string> {
  const root = await realpath(cwd);
  const top = await realpath(await git(root, ["rev-parse", "--show-toplevel"]));
  const expected = await realpath(expectedRoot);
  if (root !== top) throw new Error("DAG planning commands must run at the repository root");
  if (top !== expected) throw new Error("The active focus belongs to a different repository");
  return root;
}

async function requireFocus(getActiveFocus: IntegrationOptions["getActiveFocus"], ctx: any): Promise<ActiveFocus> {
  const focus = await getActiveFocus(ctx);
  if (!focus || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(focus.id) || !focus.repositoryRoot) throw new Error("No exact active project-model focus; run /dag brainstorm first");
  return focus;
}

async function activeFocusHeads(store: DagPlanningStoreV1, focusId: string): Promise<DagPlanningPlanV1[]> {
  const plans: DagPlanningPlanV1[] = [];
  for (const summary of await store.list()) {
    const plan = await store.read(summary.planId);
    if (plan.focusId === focusId && plan.status !== "superseded") plans.push(plan);
  }
  return plans.sort((left, right) => left.planId.localeCompare(right.planId));
}

async function eligibleFocusHeads(store: DagPlanningStoreV1, focusId: string): Promise<DagPlanningPlanV1[]> {
  return (await activeFocusHeads(store, focusId)).filter(isRunEligible);
}

function isRunEligible(plan: DagPlanningPlanV1): boolean {
  return plan.status === "ready" && plan.approval.status === "approved" && plan.authorization.status === "authorized";
}

function assertPlanFocus(plan: DagPlanningPlanV1, focus: ActiveFocus): void {
  if (plan.focusId !== focus.id) throw new Error(`DAG plan ${plan.planId}@${plan.revision} does not match active focus ${focus.id}`);
}

async function assertSelectedHead(store: DagPlanningStoreV1, selected: DagPlanningPlanV1): Promise<void> {
  const head = await store.read(selected.planId);
  if (head.revision !== selected.revision || head.planHash !== selected.planHash) throw new Error(`DAG plan selector must name the exact current head ${head.planId}@${head.revision}`);
}

function bindPlan(pi: ExtensionAPI, ctx: any, repositoryRoot: string, plan: DagPlanningPlanV1): void {
  const binding: PlanningSessionBindingV1 = {
    repositoryRoot,
    sessionId: String(ctx.sessionManager.getSessionId()),
    planId: plan.planId,
    revision: plan.revision,
    planHash: plan.planHash,
  };
  pi.appendEntry(PLAN_BINDING_ENTRY, binding);
}

function latestPlanBinding(ctx: any, repositoryRoot: string): PlanningSessionBindingV1 | null {
  const sessionId = String(ctx.sessionManager.getSessionId());
  const entries = ctx.sessionManager.getBranch?.() ?? ctx.sessionManager.getEntries?.() ?? [];
  for (let index = entries.length - 1; index >= 0; index -= 1) {
    const entry = entries[index];
    if (entry?.type !== "custom" || entry.customType !== PLAN_BINDING_ENTRY) continue;
    const value = entry.data as PlanningSessionBindingV1;
    if (value?.repositoryRoot === repositoryRoot && value.sessionId === sessionId) return value;
  }
  return null;
}

async function readBoundPlan(store: DagPlanningStoreV1, binding: PlanningSessionBindingV1): Promise<DagPlanningPlanV1> {
  const plan = await store.read(binding.planId, binding.revision);
  if (plan.planHash !== binding.planHash) throw new Error("Current-session plan binding does not match its exact static plan");
  return plan;
}

function normalizeHandleArguments(restOrParsed: string | ParsedCommandInput, optionsOrCtx: any, maybeCtx: any): { rest: string; commandOptions: Record<string, string | boolean>; ctx: any } {
  if (maybeCtx) return { rest: typeof restOrParsed === "string" ? restOrParsed : restOrParsed.rest, commandOptions: optionsOrCtx ?? {}, ctx: maybeCtx };
  if (typeof restOrParsed === "object") return { rest: restOrParsed.rest ?? "", commandOptions: restOrParsed.options ?? {}, ctx: optionsOrCtx };
  const parsed = parseRestAndOptions(restOrParsed);
  return { rest: parsed.rest, commandOptions: parsed.options, ctx: optionsOrCtx };
}

function parseRestAndOptions(input: string): ParsedCommandInput {
  const tokens = splitArgs(input);
  const positional: string[] = [];
  const options: Record<string, string | boolean> = {};
  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index];
    if (!token.startsWith("--")) { positional.push(token); continue; }
    const [name, inline] = token.slice(2).split("=", 2);
    if (inline !== undefined) { options[name] = inline; continue; }
    if (name === "new" || name === "resume") { options[name] = true; continue; }
    const next = tokens[index + 1];
    if (next && !next.startsWith("--")) { options[name] = next; index += 1; }
    else options[name] = true;
  }
  return { rest: positional.join(" "), options };
}

function splitArgs(input: string): string[] {
  const values: string[] = [];
  const pattern = /"([^"]*)"|'([^']*)'|([^\s]+)/g;
  let match: RegExpExecArray | null;
  while ((match = pattern.exec(input))) values.push(match[1] ?? match[2] ?? match[3]);
  return values;
}

function exactRevisionOption(value: string | boolean | undefined): string {
  if (typeof value !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}@[1-9][0-9]*$/.test(value)) throw new Error("--plan must name one exact planId@revision");
  return value;
}

function sessionActor(ctx: any): string {
  const id = String(ctx.sessionManager.getSessionId());
  if (!id.trim()) throw new Error("A current session identity is required for a plan decision");
  return `session:${id}`;
}

function decisionSummary(plan: DagPlanningPlanV1): string {
  return `Plan ${plan.planId}@${plan.revision}: approval ${plan.approval.status}; authorization ${plan.authorization.status}. Execution not started.`;
}

function runSummary(binding: DagSessionRunBindingV1, state: any, projection: any): string {
  const nodeCount = Array.isArray(projection?.nodes) ? projection.nodes.length : Object.keys(state.workItems ?? {}).length;
  return `DAG run ${binding.runId} is ${state.current.run} at revision ${state.revision}; ${nodeCount} work items. Call dag_next_action for the full current semantic choices, invoke one, then refresh. Never use generic subagent for canonical DAG work; no lifecycle mutation occurs until the agent invokes a named DAG tool.`;
}

function assertSafeRepositoryPath(root: string, path: string): void {
  const segments = path.split("/");
  if (!path || path.startsWith("/") || path.startsWith("\\") || path.includes("\\") || path.includes("//") || segments.some((segment) => !segment || segment === "." || segment === "..")) throw new Error(`Unsafe planning source path: ${path}`);
  const absolute = resolve(root, path);
  const rel = relative(root, absolute);
  if (!rel || rel.startsWith("..") || rel.includes(`..${sep}`)) throw new Error(`Unsafe planning source path: ${path}`);
}

async function git(cwd: string, args: string[]): Promise<string> {
  const result = await run("git", args, { cwd, encoding: "utf8", maxBuffer: 4 * 1024 * 1024, env: gitEnv() });
  return result.stdout.trim();
}

async function gitBuffer(cwd: string, args: string[]): Promise<Buffer> {
  const result = await run("git", args, { cwd, encoding: "buffer", maxBuffer: 16 * 1024 * 1024, env: gitEnv() });
  return result.stdout as Buffer;
}

function gitEnv(): NodeJS.ProcessEnv {
  return { ...process.env, LC_ALL: "C", LANG: "C", GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null", GIT_TERMINAL_PROMPT: "0", GIT_OPTIONAL_LOCKS: "0" };
}

function hashBytes(bytes: Uint8Array): string {
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
}

function registerTool(pi: ExtensionAPI, definition: { name: string; label: string; description: string; parameters: any; execute(params: any, ctx: any): Promise<any> }): void {
  pi.registerTool({
    ...definition,
    async execute(_toolCallId, params, _signal, _onUpdate, ctx) { return definition.execute(params, ctx); },
  });
}
