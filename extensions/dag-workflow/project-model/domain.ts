import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import {
  MODEL_COLLECTIONS,
  allocateObjectId,
  allObjects,
  assertValidProjectModel,
  canonicalStringify,
  findObject,
  modelHash,
  nowIso,
  isGoverningState,
  specEligibleObjectIds,
} from "./model.ts";
import { assertFreshMigrationReadiness, materializeMigrationMetadata, migrationReadinessErrors, type MigrationMetadataInput } from "./migration-workflow.ts";
import { SpecProjector } from "./projector.ts";
import { reviewArtifactDigest, type ModelReviewTurnProjection, type PresentationBlock } from "./review-turn.ts";
import { FocusSessionStore } from "./sessions.ts";
import { ProjectModelStore } from "./store.ts";
import { pendingPoints, requireReview, validateOperationScope } from "./reviews.ts";
import type {
  CurrentUnderstanding,
  ModelReviewContent,
  ModelOperationScope,
  ModelReview,
  ModelCollectionName,
  ModelObject,
  ModelObjectBase,
  ProjectModel,
  ReviewDirection,
  ReviewPoint,
  SpecProjectionView,
} from "./types.ts";

const DEFAULT_STATE: Record<ModelCollectionName, string> = {
  workstreams: "active",
  intents: "proposed",
  concepts: "proposed",
  evidence: "current",
  assumptions: "open",
  questions: "open",
  tensions: "active",
  scenarios: "proposed",
  proposals: "candidate",
  decisions: "candidate",
  commitments: "not_reviewed",
  discoveries: "untriaged",
};

const DIRECTION_COLLECTIONS = new Set<ModelCollectionName>(["intents", "concepts", "scenarios", "decisions", "commitments"]);
const ACCEPTED_STATE: Partial<Record<ModelCollectionName, string>> = {
  intents: "accepted",
  concepts: "accepted",
  scenarios: "accepted",
  decisions: "accepted",
  commitments: "accepted",
};

export interface AddObjectInput {
  collection: ModelCollectionName;
  id?: string;
  key?: string;
  value: Record<string, unknown>;
}

export interface PatchObjectInput {
  id: string;
  changes: Record<string, unknown>;
}

export interface ModelUpdateInput {
  add?: AddObjectInput[];
  patch?: PatchObjectInput[];
  removeIds?: string[];
  currentUnderstanding?: { body: string; sourceObjectIds: string[] };
  specViews?: SpecProjectionView[];
  migration?: MigrationMetadataInput;
}

export type DirectionInput = ReviewDirection;

export interface ReviewPointInput {
  id?: string;
  key?: string;
  title: string;
  context: string;
  purpose: "awareness" | "decision";
  question?: string;
  objectIds?: string[];
  options?: Array<{ id?: string; key?: string; label: string; description: string; objectId?: string; recommended?: boolean; rationale?: string; direction?: DirectionInput }>;
  rejectDirection?: DirectionInput;
  deferDirection?: DirectionInput;
}

export interface ReviewOutcomeInput {
  pointId: string;
  action: "accept" | "reject" | "modify" | "defer" | "unresolved";
  optionId?: string;
  direction?: DirectionInput;
}

export class ProjectModelDomain {
  readonly root: string;
  readonly models: ProjectModelStore;
  readonly sessions: FocusSessionStore;
  readonly projector: SpecProjector;

  constructor(root: string) {
    this.root = root;
    this.models = new ProjectModelStore(root);
    this.sessions = new FocusSessionStore(root);
    this.projector = new SpecProjector(root);
  }

  async context(scope: ModelOperationScope, input: { view?: string; ids?: string[]; reviewId?: string; legacyFocusId?: string } = {}) {
    const model = await this.models.load();
    validateOperationScope(model, scope);
    const view = input.view ?? "orientation";
    if (view === "reviews") return this.pendingReviews(scope);
    if (view === "legacy_review") {
      if (!input.legacyFocusId || !input.reviewId) throw new Error("Historical review requires exact legacyFocusId and reviewId");
      const historical = await this.sessions.load(input.legacyFocusId);
      if (historical.id !== input.legacyFocusId || historical.activeReview?.id !== input.reviewId) throw new Error("Historical review address not found");
      return { readOnly: true, legacyFocusId: historical.id, review: historical.activeReview };
    }
    if (view === "review") {
      const review = requireReview(model, input.reviewId!);
      return { review, markdown: renderReviewMarkdown(model, { ...review, points: pendingPoints(review) }) };
    }
    if (view === "orientation") return {
      project: { id: model.project.id, title: model.project.title, revision: model.project.revision, mode: model.project.mode },
      scope,
      currentUnderstanding: model.project.currentUnderstanding ?? null,
      counts: scopedCounts(model, scope),
      pendingReviews: await this.pendingReviews(scope),
      ...(model.project.migration ? { migration: { phase: model.project.migration.phase, blockers: model.project.migration.blockers, readinessErrors: migrationReadinessErrors(model) } } : {}),
    };
    if (view === "migration") return model.project.migration ? { metadata: model.project.migration, readinessErrors: migrationReadinessErrors(model) } : null;
    if (view === "entities") {
      if (!input.ids?.length) throw new Error("entities context requires ids");
      return input.ids.map((id) => {
        const found = findObject(model, id);
        if (!found) throw new Error(`Unknown model object: ${id}`);
        return found;
      });
    }
    if (view === "frontier") return scopedObjects(model, scope).filter(isUnresolved);
    if (view === "governing") {
      const eligible = specEligibleObjectIds(model);
      return scopedObjects(model, scope).filter(({ object }) => eligible.has(object.id));
    }
    throw new Error(`Unknown model context view: ${view}`);
  }

  async pendingReviews(scope: ModelOperationScope) {
    const model = await this.models.load();
    validateOperationScope(model, scope);
    const relevant = (reviewScope: ModelOperationScope) => !scope.workstreamIds.length || !reviewScope.workstreamIds.length ||
      reviewScope.workstreamIds.some((id) => scope.workstreamIds.includes(id)) ||
      (reviewScope.objectIds ?? []).some((id) => scope.objectIds?.includes(id));
    const records = (model.project.reviews ?? []).filter((review) => review.status === "pending" && relevant(review.scope))
      .map((review) => ({ reviewId: review.id, revision: review.revision, title: review.title, scope: review.scope, pendingPointIds: pendingPoints(review).map(({ id }) => id), readOnly: false }));
    // Legacy records are discoverable by exact compound address, never resumed or rewritten.
    const legacy = [];
    for (const summary of await this.sessions.list()) {
      const session = await this.sessions.load(summary.id);
      if (session.activeReview && relevant(session)) legacy.push({ reviewId: session.activeReview.id, legacyFocusId: session.id, title: session.activeReview.title, scope: { workstreamIds: session.workstreamIds }, readOnly: true });
    }
    return [...records, ...legacy];
  }

  async reviewTurn(reviewId: string, presentationBlocks: PresentationBlock[] = []): Promise<ModelReviewTurnProjection> {
    const model = await this.models.load();
    const record = requireReview(model, reviewId);
    const review = projectReview({ ...record, points: pendingPoints(record) });
    return {
      schemaVersion: 2,
      project: { id: model.project.id, title: model.project.title, revision: model.project.revision, modelHash: modelHash(model) },
      scope: record.scope,
      currentUnderstanding: { body: model.project.currentUnderstanding?.body ?? "No Current understanding has been recorded yet." },
      context: { createdAtModelRevision: record.modelRevision, pendingPointCount: review.points.length },
      frontier: scopedObjects(model, record.scope).filter(isUnresolved).map(({ collection, object }) => ({ id: object.id, type: collection, title: object.title, state: object.state, summary: object.body, badges: [collection, object.state] })),
      frontierHandoff: "Review relevant unresolved decisions using contextual user intent; omitted or ambiguous points remain pending.",
      review: { ...review, revision: record.revision, artifactDigest: reviewArtifactDigest(review) },
      ...(presentationBlocks.length ? { presentationBlocks: structuredClone(presentationBlocks) } : {}),
    };
  }

  async update(scope: ModelOperationScope, input: ModelUpdateInput) {
    const result = await this.transact(async (draft, changed) => {
      validateOperationScope(draft, scope);
      applyUpdate(draft, scope, input, changed);
      if (input.migration) {
        draft.project.migration = await materializeMigrationMetadata(this.root, draft, input.migration);
        changed.add("project.migration");
      }
    });
    return operationResult("update", result, { scope });
  }

  async recordDirection(scope: ModelOperationScope, input: { directions?: DirectionInput[]; currentUnderstanding?: ModelUpdateInput["currentUnderstanding"]; specViews?: SpecProjectionView[]; cutover?: boolean }) {
    if (input.cutover) {
      if (input.directions?.length || input.currentUnderstanding || input.specViews) throw new Error("Migration cutover must be an isolated operation");
      return this.cutover(scope);
    }
    if (!input.directions?.length) throw new Error("record_direction requires directions");
    const result = await this.transact((draft, changed) => {
      validateOperationScope(draft, scope);
      if (input.specViews) { draft.project.projections.specs = structuredClone(input.specViews); changed.add("project.projections"); }
      for (const direction of input.directions!) applyDirection(draft, scope, direction, changed);
      if (input.currentUnderstanding) setCurrentUnderstanding(draft, input.currentUnderstanding, changed);
    });
    return operationResult("record_direction", result, { scope });
  }

  async cutover(scope: ModelOperationScope) {
    const options = { replaceUnmanagedSpecs: false };
    const result = await this.transact(async (draft, changed) => {
      validateOperationScope(draft, scope);
      if (draft.project.mode !== "candidate") throw new Error("Project model is already authoritative");
      if (draft.project.migration) {
        await assertFreshMigrationReadiness(this.root, draft);
        options.replaceUnmanagedSpecs = true;
      }
      for (const { collection, object } of allObjects(draft)) {
        const acceptedState = ACCEPTED_STATE[collection];
        if (acceptedState && ["proposed", "not_reviewed", "candidate"].includes(object.state)) {
          (object as ModelObjectBase).state = acceptedState;
          object.updatedAt = nowIso();
          changed.add(object.id);
        }
      }
      draft.project.mode = "authoritative";
      changed.add("project.mode");
    }, options);
    return operationResult("migration_cutover", result, { scope });
  }

  async createReview(scope: ModelOperationScope, input: { id?: string; key?: string; title: string; points: ReviewPointInput[] }) {
    let review!: ModelReview;
    const result = await this.transact((draft, changed) => {
      validateOperationScope(draft, scope);
      if (!input.points?.length) throw new Error("Review requires points");
      const id = normalizeNestedId("review", input.id ?? input.key ?? input.title);
      if (draft.project.reviews?.some((record) => record.id === id)) throw new Error(`Review already exists: ${id}`);
      const reserved = new Set(allObjects(draft).map(({ object }) => object.id));
      for (const record of draft.project.reviews ?? []) for (const point of pendingPoints(record)) for (const direction of reviewDirections(point)) if (direction.newId) reserved.add(direction.newId);
      review = { id, title: input.title, createdAt: nowIso(), revision: 0, scope: structuredClone(scope), modelRevision: draft.project.revision, status: "pending", outcomes: [], points: input.points.map((point) => normalizeReviewPoint(draft, scope, point, reserved)) };
      assertUniqueReviewIds(review);
      (draft.project.reviews ??= []).push(review);
      changed.add(review.id);
    });
    return { ...operationResult("review", result, { reviewId: review.id }), review, markdown: renderReviewMarkdown(result.model, review), next: "Present the decisions; resolve only the points settled by contextual user direction." };
  }

  async markReviewPresented(reviewId: string, expectedRevision: number) {
    if (!Number.isSafeInteger(expectedRevision) || expectedRevision < 0) throw new Error("Review expectedRevision is required");
    const result = await this.transact((draft, changed) => {
      const review = requireReview(draft, reviewId, expectedRevision);
      review.presentedAt = nowIso();
      review.revision += 1;
      changed.add(review.id);
    });
    return requireReview(result.model, reviewId);
  }

  async resolveReview(input: { reviewId: string; expectedRevision: number; outcomes?: ReviewOutcomeInput[]; update?: ModelUpdateInput; currentUnderstanding?: ModelUpdateInput["currentUnderstanding"] }) {
    if (!Number.isSafeInteger(input.expectedRevision) || input.expectedRevision < 0) throw new Error("Review expectedRevision is required");
    const appliedPointIds: string[] = [];
    const result = await this.transact(async (draft, changed) => {
      const review = requireReview(draft, input.reviewId, input.expectedRevision);
      if (review.status !== "pending") throw new Error(`Review is already resolved: ${review.id}`);
      const outcomes = input.outcomes ?? [];
      if (new Set(outcomes.map(({ pointId }) => pointId)).size !== outcomes.length) throw new Error("Review outcomes contain duplicate point IDs");
      for (const outcome of outcomes) {
        const point = pendingPoints(review).find(({ id }) => id === outcome.pointId);
        if (!point) throw new Error(`Review outcome references unknown or resolved point: ${outcome.pointId}`);
        if (outcome.action === "unresolved") { if (outcome.direction) throw new Error("Unresolved outcome cannot carry direction"); continue; }
        let direction: DirectionInput | undefined;
        if (outcome.action === "accept") {
          const option = point.options.find(({ id }) => id === outcome.optionId);
          if (point.purpose === "decision" && !option) throw new Error(`${point.id} acceptance requires a valid optionId`);
          direction = outcome.direction ?? option?.direction;
        } else if (outcome.action === "modify") {
          if (!outcome.direction) throw new Error(`${point.id} modification requires explicit direction`);
          direction = outcome.direction;
        } else if (outcome.action === "reject" || outcome.action === "defer") {
          direction = outcome.direction ?? (outcome.action === "reject" ? point.rejectDirection : point.deferDirection);
        } else throw new Error(`Invalid review outcome action: ${outcome.action}`);
        if (direction) applyDirection(draft, review.scope, direction, changed);
        review.outcomes.push({ ...structuredClone(outcome), action: outcome.action, ...(direction ? { direction: structuredClone(direction) } : {}), recordedAt: nowIso() });
        appliedPointIds.push(point.id);
      }
      if (input.update) {
        applyUpdate(draft, review.scope, input.update, changed);
        if (input.update.migration) draft.project.migration = await materializeMigrationMetadata(this.root, draft, input.update.migration);
      }
      if (input.currentUnderstanding) setCurrentUnderstanding(draft, input.currentUnderstanding, changed);
      review.status = pendingPoints(review).length ? "pending" : "resolved";
      review.revision += 1;
      changed.add(review.id);
    });
    const review = requireReview(result.model, input.reviewId);
    return operationResult("resolve_review", result, { reviewId: review.id, reviewRevision: review.revision, appliedPointIds, unresolvedPointIds: pendingPoints(review).map(({ id }) => id), ...(review.status === "pending" ? { remainingReview: { review, markdown: renderReviewMarkdown(result.model, { ...review, points: pendingPoints(review) }) } } : {}) });
  }

  async specs(input: { action: "preview" | "check" | "generate"; outputDirectory?: string; replaceUnmanaged?: boolean; removeStale?: boolean }) {
    return this.models.withSnapshot(async (model) => {
      if (input.action === "preview") {
        const preview = await this.projector.preview(model, input.outputDirectory);
        return { action: "specs.preview", revision: model.project.revision, modelHash: modelHash(model), directory: preview.directory, files: preview.rendered.map(({ path, inputDigest }) => ({ path, inputDigest })) };
      }
      if (input.action === "check") {
        const check = await this.projector.check(model);
        return { action: "specs.check", revision: model.project.revision, modelHash: modelHash(model), driftPaths: check.driftPaths, stalePaths: check.stalePaths, ok: !check.driftPaths.length && !check.stalePaths.length };
      }
      const generated = await this.projector.generate(model, { replaceUnmanaged: input.replaceUnmanaged, removeStale: input.removeStale });
      return { action: "specs.generate", revision: model.project.revision, modelHash: modelHash(model), changedPaths: generated.changedPaths, stalePaths: generated.stalePaths };
    });
  }

  private async transact(
    mutator: (draft: ProjectModel, changed: Set<string>) => void | Promise<void>,
    options: { replaceUnmanagedSpecs?: boolean } = {},
  ) {
    const changed = new Set<string>();
    let changedPaths: string[] = [];
    let stalePaths: string[] = [];
    let backups = new Map<string, string | undefined>();
    const result = await this.models.mutate(async (draft) => {
      const current = structuredClone(draft);
      await mutator(draft, changed);
      assertValidProjectModel(draft);
      this.projector.render(draft);
      if (draft.project.mode === "authoritative") {
        backups = await backupPaths(this.root, [...new Set([...this.projector.targetPaths(current), ...this.projector.targetPaths(draft)])]);
        const generated = await this.projector.generate(draft, { replaceUnmanaged: options.replaceUnmanagedSpecs });
        changedPaths = generated.changedPaths;
        stalePaths = generated.stalePaths;
      }
    }, { onError: () => restorePaths(this.root, backups) });
    return { model: result.model, changedIds: [...changed].sort(), changedPaths, stalePaths };
  }
}

function applyUpdate(model: ProjectModel, scope: ModelOperationScope, input: ModelUpdateInput, changed: Set<string>) {
  for (const addition of input.add ?? []) {
    if (!MODEL_COLLECTIONS.includes(addition.collection)) throw new Error(`Unknown model collection: ${addition.collection}`);
    const object = normalizeNewObject(model, scope, addition);
    (model[addition.collection] as ModelObject[]).push(object);
    changed.add(object.id);
  }
  for (const patch of input.patch ?? []) {
    const found = findObject(model, patch.id);
    if (!found) throw new Error(`Unknown model object: ${patch.id}`);
    const forbidden = ["acceptance", "introducedBy", "createdAt", "updatedAt", "id"].filter((key) => key in patch.changes);
    if (forbidden.length) throw new Error(`dag_model_update cannot set authority-controlled fields: ${forbidden.join(", ")}`);
    if (isGoverningState(found.collection, found.object.state) && touchesSemanticFields(patch.changes)) throw new Error(`dag_model_update cannot change accepted semantic content: ${patch.id}`);
    if ("state" in patch.changes && isGoverningState(found.collection, String(patch.changes.state))) throw new Error(`dag_model_update cannot grant governing state: ${patch.id}`);
    Object.assign(found.object, structuredClone(patch.changes), { updatedAt: nowIso() });
    changed.add(found.object.id);
  }
  for (const id of input.removeIds ?? []) {
    const found = findObject(model, id);
    if (!found) throw new Error(`Unknown model object: ${id}`);
    if (isGoverningState(found.collection, found.object.state)) throw new Error(`Accepted object cannot be hard-deleted: ${id}`);
    if (allObjects(model).some(({ object }) => object.id !== id && object.relationships.some(({ targetId }) => targetId === id))) throw new Error(`Referenced object cannot be hard-deleted: ${id}`);
    if (model.project.projections.specs.some((view) => view.sections?.some((section) => section.objectIds.includes(id)))) throw new Error(`Projected object cannot be hard-deleted: ${id}`);
    model[found.collection] = (model[found.collection] as ModelObject[]).filter((object) => object.id !== id) as never;
    changed.add(id);
  }
  if (input.specViews) {
    model.project.projections.specs = structuredClone(input.specViews);
    changed.add("project.projections");
  }
  if (input.currentUnderstanding) setCurrentUnderstanding(model, input.currentUnderstanding, changed);
}

function normalizeNewObject(model: ProjectModel, scope: ModelOperationScope, input: AddObjectInput, introducedBy: "agent" | "user" = "agent"): ModelObject {
  const createdAt = nowIso();
  const value = sanitizeAuthorityFields(input.value) as unknown as ModelObject;
  const id = input.id ?? allocateObjectId(model, input.collection, input.key ?? String(value.title ?? input.collection));
  if (findObject(model, id)) throw new Error(`Duplicate model object: ${id}`);
  const state = String(value.state ?? DEFAULT_STATE[input.collection]);
  if (introducedBy === "agent" && isGoverningState(input.collection, state)) throw new Error(`dag_model_update cannot create governing ${input.collection} state ${state}`);
  const objectScope = value.scope ?? (scope.workstreamIds.length ? { kind: "workstreams", workstreamIds: scope.workstreamIds } : { kind: "repository" });
  return {
    ...value,
    id,
    title: String(value.title ?? "").trim(),
    body: String(value.body ?? "").trim(),
    state,
    scope: objectScope,
    introducedBy,
    sourceRefs: value.sourceRefs ?? [],
    relationships: value.relationships ?? [],
    createdAt,
    updatedAt: createdAt,
  } as ModelObject;
}

function applyDirection(
  model: ProjectModel,
  scope: ModelOperationScope,
  input: DirectionInput,
  changed: Set<string>,
): ModelObject {
  if (!DIRECTION_COLLECTIONS.has(input.collection)) throw new Error(`Direct direction cannot target ${input.collection}`);
  if (input.id && input.newId) throw new Error("Direction cannot set both id and newId");
  let object: ModelObject;
  if (input.id) {
    const found = findObject(model, input.id);
    if (!found || found.collection !== input.collection) throw new Error(`Direction target not found in ${input.collection}: ${input.id}`);
    object = found.object;
    if (input.value) Object.assign(object, sanitizeAuthorityFields(input.value));
  } else {
    if (!input.value) throw new Error("New direction requires value");
    object = normalizeNewObject(model, scope, { collection: input.collection, id: input.newId, key: input.key, value: input.value as Record<string, unknown> }, "user");
    (model[input.collection] as ModelObject[]).push(object);
  }
  (object as ModelObjectBase).state = input.state ?? ACCEPTED_STATE[input.collection]!;
  object.updatedAt = nowIso();
  // Historical receipts remain historical; new direction grants state, not a proof of consent.
  changed.add(object.id);
  return object;
}

function reviewDirections(point: ReviewPoint): DirectionInput[] {
  return [...point.options.map(({ direction }) => direction), point.rejectDirection, point.deferDirection].filter((direction): direction is DirectionInput => Boolean(direction));
}

function setCurrentUnderstanding(model: ProjectModel, input: { body: string; sourceObjectIds: string[] }, changed: Set<string>) {
  if (!input.body?.trim()) throw new Error("Current understanding body is required");
  const sourceObjects = [...new Set(input.sourceObjectIds ?? [])].map((id) => {
    const found = findObject(model, id);
    if (!found) throw new Error(`Current understanding references missing object: ${id}`);
    return { id };
  });
  model.project.currentUnderstanding = { body: input.body.trim(), generatedAt: nowIso(), sourceObjects } satisfies CurrentUnderstanding;
  changed.add("project.currentUnderstanding");
}

function normalizeReviewPoint(model: ProjectModel, scope: ModelOperationScope, input: ReviewPointInput, reservedIds: Set<string>): ReviewPoint {
  const rejectDirection = input.rejectDirection ? materializeReviewDirection(model, scope, input.rejectDirection, reservedIds) : undefined;
  const deferDirection = input.deferDirection ? materializeReviewDirection(model, scope, input.deferDirection, reservedIds) : undefined;
  const directionTargetIds = [rejectDirection?.id, deferDirection?.id].filter(Boolean) as string[];
  const point: ReviewPoint = {
    id: normalizeNestedId("point", input.id ?? input.key ?? input.title),
    title: input.title.trim(),
    context: input.context.trim(),
    purpose: input.purpose,
    ...(input.question?.trim() ? { question: input.question.trim() } : {}),
    objectRefs: [...new Set([...(input.objectIds ?? []), ...directionTargetIds])].map((id) => {
      const found = findObject(model, id);
      if (!found) throw new Error(`Review references missing object: ${id}`);
      return { id };
    }),
    options: (input.options ?? []).map((option) => {
      if (option.objectId && !findObject(model, option.objectId)) throw new Error(`Review option references missing object: ${option.objectId}`);
      const direction = option.direction ? materializeReviewDirection(model, scope, option.direction, reservedIds) : undefined;
      const normalized = {
        id: normalizeNestedId("option", option.id ?? option.key ?? option.label),
        label: option.label,
        description: option.description,
        ...(option.objectId ? { objectId: option.objectId } : {}),
        ...(option.recommended ? { recommended: true } : {}),
        ...(option.rationale ? { rationale: option.rationale } : {}),
        ...(direction ? { direction } : {}),
      };
      return normalized;
    }),
    ...(rejectDirection ? { rejectDirection } : {}),
    ...(deferDirection ? { deferDirection } : {}),
  };
  if (point.purpose === "decision" && (!point.question || !point.options.length)) throw new Error(`${point.id} decision requires a question and options`);
  return point;
}

function isUnresolved({ collection, object }: ReturnType<typeof allObjects>[number]) {
  return (collection === "questions" && ["open", "deferred"].includes(object.state)) ||
    (collection === "tensions" && ["active", "deferred"].includes(object.state)) ||
    (collection === "discoveries" && ["untriaged", "investigating", "deferred"].includes(object.state)) ||
    (DIRECTION_COLLECTIONS.has(collection) && ["candidate", "proposed", "not_reviewed", "disputed"].includes(object.state));
}

function scopedObjects(model: ProjectModel, scope: ModelOperationScope) {
  const selected = new Set(scope.workstreamIds);
  return allObjects(model).filter(({ object }) =>
    (scope.objectIds?.includes(object.id) || object.scope.kind === "repository" || object.scope.workstreamIds.some((id) => selected.has(id))),
  );
}

function scopedCounts(model: ProjectModel, scope: ModelOperationScope) {
  const counts: Record<string, number> = {};
  for (const { collection } of scopedObjects(model, scope)) counts[collection] = (counts[collection] ?? 0) + 1;
  return counts;
}

function projectReview(review: ModelReviewContent) {
  return {
    id: review.id,
    title: review.title,
    points: review.points.map(({ rejectDirectionValuePatch: _rejectPatch, deferDirectionValuePatch: _deferPatch, ...point }) => ({
      ...point,
      options: point.options.map(({ directionValuePatch: _directionPatch, ...option }) => option),
    })),
  };
}

function renderReviewMarkdown(model: ProjectModel, review: ModelReviewContent): string {
  const lines = [`# ${review.title}`];
  if (model.project.currentUnderstanding) lines.push("", "## Current understanding", "", model.project.currentUnderstanding.body);
  const awareness = review.points.filter(({ purpose }) => purpose === "awareness");
  const decisions = review.points.filter(({ purpose }) => purpose === "decision");
  if (awareness.length) {
    lines.push("", "## For awareness");
    for (const point of awareness) lines.push("", `### ${point.title}`, "", point.context);
  }
  if (decisions.length) {
    lines.push("", "## Decisions needed");
    for (const point of decisions) {
      lines.push("", `### ${point.title}`, "", point.context, "", `**${point.question}**`, "");
      for (const option of point.options) {
        lines.push(`- **${option.label}${option.recommended ? " (Recommended)" : ""}:** ${option.description}${option.rationale ? ` _${option.rationale}_` : ""}`);
        if (option.direction) lines.push("  - Commits this exact authority payload:", "", "```json", prettyDirection(option.direction), "```");
      }
      if (point.rejectDirection) lines.push("", "**Reject commits this exact authority payload:**", "", "```json", prettyDirection(point.rejectDirection), "```");
      if (point.deferDirection) lines.push("", "**Defer commits this exact authority payload:**", "", "```json", prettyDirection(point.deferDirection), "```");
      lines.push("- **Other:** Provide another explicit direction.");
    }
  }
  return `${lines.join("\n")}\n`;
}

function operationResult<T extends Record<string, unknown>>(action: string, result: { model: ProjectModel; changedIds: string[]; changedPaths: string[]; stalePaths: string[] }, extra: T) {
  return {
    action,
    revision: result.model.project.revision,
    modelHash: modelHash(result.model),
    changedIds: result.changedIds,
    generatedPaths: result.changedPaths,
    staleGeneratedPaths: result.stalePaths,
    ...extra,
  } as const;
}

function normalizeNestedId(prefix: string, value: string): string {
  const slug = String(value).trim().toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 64) || prefix;
  return slug.startsWith(`${prefix}-`) ? slug : `${prefix}-${slug}`;
}

function materializeReviewDirection(model: ProjectModel, scope: ModelOperationScope, input: DirectionInput, reservedIds: Set<string>): DirectionInput {
  validateReviewDirection(input);
  let object: ModelObject;
  let id: string | undefined;
  let newId: string | undefined;
  if (input.id) {
    const found = findObject(model, input.id);
    if (!found || found.collection !== input.collection) throw new Error(`Review direction target not found in ${input.collection}: ${input.id}`);
    object = structuredClone(found.object);
    if (input.value) Object.assign(object, sanitizeAuthorityFields(input.value as Record<string, unknown>));
    id = input.id;
  } else {
    const basis = input.key ?? String(input.value?.title ?? input.collection);
    newId = input.newId ?? allocateReservedObjectId(model, input.collection, basis, reservedIds);
    if (reservedIds.has(newId)) throw new Error(`Review direction object ID is already reserved: ${newId}`);
    object = normalizeNewObject(model, scope, { collection: input.collection, id: newId, key: input.key, value: input.value as Record<string, unknown> }, "user");
    reservedIds.add(newId);
  }
  (object as ModelObjectBase).state = input.state ?? ACCEPTED_STATE[input.collection]!;
  // Applicability is checked when proposing and applying, not on every future model write.
  const preview = structuredClone(model);
  delete preview.project.reviews;
  preview.project.mode = "candidate";
  if (id) Object.assign(findObject(preview, id)!.object, object);
  else (preview[input.collection] as ModelObject[]).push(object);
  assertValidProjectModel(preview);
  const { acceptance: _acceptance, createdAt: _createdAt, id: _id, introducedBy: _introducedBy, state: _state, updatedAt: _updatedAt, ...value } = object;
  return { collection: input.collection, ...(id ? { id } : { newId }), ...(input.key ? { key: input.key } : {}), state: object.state, value };
}

function allocateReservedObjectId(model: ProjectModel, collection: ModelCollectionName, basis: string, reservedIds: Set<string>): string {
  const base = allocateObjectId(model, collection, basis);
  if (!reservedIds.has(base)) return base;
  let suffix = 2;
  while (reservedIds.has(`${base}-${suffix}`)) suffix += 1;
  return `${base}-${suffix}`;
}

function validateReviewDirection(direction: DirectionInput) {
  if (!DIRECTION_COLLECTIONS.has(direction.collection)) throw new Error(`Review authority payload cannot target ${direction.collection}`);
  if (direction.id && direction.newId) throw new Error("Review authority payload cannot set both id and newId");
  if (!direction.id && !direction.value) throw new Error("New review authority payload requires value");
  if (direction.value) {
    const forbidden = ["acceptance", "introducedBy", "createdAt", "updatedAt", "id"].filter((key) => key in direction.value!);
    if (forbidden.length) throw new Error(`Review authority payload cannot set controlled fields: ${forbidden.join(", ")}`);
  }
}

function assertUniqueReviewIds(review: ModelReviewContent) {
  const pointIds = new Set<string>();
  for (const point of review.points) {
    if (pointIds.has(point.id)) throw new Error(`Duplicate normalized review point id: ${point.id}`);
    pointIds.add(point.id);
    const optionIds = new Set<string>();
    for (const option of point.options) {
      if (optionIds.has(option.id)) throw new Error(`Duplicate normalized option id in ${point.id}: ${option.id}`);
      optionIds.add(option.id);
    }
  }
}

function prettyDirection(direction: DirectionInput): string {
  return JSON.stringify(JSON.parse(canonicalStringify(direction)), null, 2);
}

function sanitizeAuthorityFields(value: Record<string, unknown>): Record<string, unknown> {
  const copy = structuredClone(value);
  for (const key of ["acceptance", "introducedBy", "createdAt", "updatedAt", "id"]) delete copy[key];
  return copy;
}

function touchesSemanticFields(changes: Record<string, unknown>): boolean {
  const metadata = new Set(["confidence", "sourceRefs", "legacyIds"]);
  return Object.keys(changes).some((key) => !metadata.has(key));
}

async function backupPaths(root: string, paths: string[]) {
  const backups = new Map<string, string | undefined>();
  for (const path of paths) {
    try { backups.set(path, await readFile(resolve(root, path), "utf8")); }
    catch (error: any) {
      if (error?.code !== "ENOENT") throw error;
      backups.set(path, undefined);
    }
  }
  return backups;
}

async function restorePaths(root: string, backups: Map<string, string | undefined>) {
  const failures: string[] = [];
  for (const [path, content] of backups) {
    const target = resolve(root, path);
    try {
      if (content === undefined) await rm(target, { force: true });
      else { await mkdir(dirname(target), { recursive: true }); await writeFile(target, content, "utf8"); }
    } catch { failures.push(path); }
  }
  if (failures.length) throw new Error(`Project-model mutation failed and projection rollback also failed: ${failures.join(", ")}`);
}
