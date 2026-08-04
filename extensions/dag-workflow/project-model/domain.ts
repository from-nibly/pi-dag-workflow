import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import {
  MODEL_COLLECTIONS,
  allocateObjectId,
  allObjects,
  assertValidProjectModel,
  candidateManifestHash,
  canonicalStringify,
  createAcceptance,
  findObject,
  modelHash,
  nowIso,
  requiresAcceptance,
  semanticHash,
  sha256,
  specEligibleObjectIds,
} from "./model.ts";
import { SpecProjector } from "./projector.ts";
import { reviewSemanticHash, type ModelReviewTurnProjection, type PresentationBlock } from "./review-turn.ts";
import { FocusSessionStore } from "./sessions.ts";
import { ProjectModelStore } from "./store.ts";
import type {
  CurrentUnderstanding,
  FocusReview,
  FocusSession,
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
  focus?: { workstreamIds: string[] };
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

  async context(focusId: string, input: { view?: string; ids?: string[] } = {}) {
    const model = await this.models.load();
    const focus = await this.sessions.load(focusId);
    const view = input.view ?? "orientation";
    if (view === "orientation") return {
      project: { id: model.project.id, title: model.project.title, revision: model.project.revision, mode: model.project.mode, hash: modelHash(model), ...(model.project.mode === "candidate" ? { candidateManifestHash: candidateManifestHash(model) } : {}) },
      focus: summarizeFocus(focus),
      currentUnderstanding: model.project.currentUnderstanding ?? null,
      counts: scopedCounts(model, focus),
      activeReview: focus.activeReview ? summarizeReview(focus.activeReview) : null,
    };
    if (view === "entities") {
      if (!(input.ids?.length)) throw new Error("entities context requires ids");
      return input.ids.map((id) => {
        const found = findObject(model, id);
        if (!found) throw new Error(`Unknown model object: ${id}`);
        return { collection: found.collection, object: found.object, semanticHash: semanticHash(found.collection, found.object) };
      });
    }
    if (view === "frontier") return scopedObjects(model, focus)
      .filter(({ collection, object }) =>
        (collection === "questions" && ["open", "deferred"].includes(object.state)) ||
        (collection === "tensions" && ["active", "deferred"].includes(object.state)) ||
        (collection === "discoveries" && ["untriaged", "investigating", "deferred"].includes(object.state)),
      )
      .map(({ collection, object }) => ({ id: object.id, collection, title: object.title, state: object.state, semanticHash: semanticHash(collection, object) }));
    if (view === "delta") return projectDelta(model, focus);
    if (view === "review") return focus.activeReview ? { review: projectReview(focus.activeReview), markdown: renderReviewMarkdown(model, focus.activeReview) } : null;
    if (view === "governing") {
      const eligibleIds = specEligibleObjectIds(model);
      return scopedObjects(model, focus)
        .filter(({ object }) => eligibleIds.has(object.id))
        .map(({ collection, object }) => ({ id: object.id, collection, title: object.title, state: object.state, semanticHash: semanticHash(collection, object) }));
    }
    throw new Error(`Unknown model context view: ${view}`);
  }

  async reviewTurn(focusId: string, presentationBlocks: PresentationBlock[] = []): Promise<ModelReviewTurnProjection> {
    const model = await this.models.load();
    const focus = await this.sessions.load(focusId);
    if (!focus.activeReview) throw new Error("No active review");
    const review = projectReview(focus.activeReview);
    const frontierObjects = scopedObjects(model, focus).filter(({ collection, object }) =>
      (collection === "questions" && ["open", "deferred"].includes(object.state)) ||
      (collection === "tensions" && ["active", "deferred"].includes(object.state)) ||
      (collection === "discoveries" && ["untriaged", "investigating", "deferred"].includes(object.state))
    );
    const delta = projectDelta(model, focus);
    const changedLabels = [...delta.added, ...delta.changed].slice(0, 8).map(({ title, state }) => `${title} (${state})`);
    return {
      schemaVersion: 1,
      project: { id: model.project.id, title: model.project.title, revision: model.project.revision, modelHash: modelHash(model) },
      focus: { id: focus.id, title: focus.title, workstreamIds: focus.workstreamIds },
      currentUnderstanding: { body: model.project.currentUnderstanding?.body ?? "No Current understanding has been recorded yet." },
      delta: {
        added: delta.added.length,
        changed: delta.changed.length,
        stillUnresolved: frontierObjects.length,
        consequences: changedLabels.length ? changedLabels : ["No scoped semantic changes since the previous presented review."],
      },
      frontier: frontierObjects.slice(0, 12).map(({ collection, object }) => ({
        id: object.id,
        type: collection,
        title: object.title,
        state: object.state,
        summary: object.body,
        badges: [collection, object.state],
      })),
      frontierHandoff: "After this turn, the agent resolves clear fresh outcomes, preserves ambiguous or stale points, updates the model, and selects the next material unresolved objects. The human can redirect the focus at any time.",
      review: { ...structuredClone(review), semanticHash: reviewSemanticHash(review) },
      ...(presentationBlocks.length ? { presentationBlocks: structuredClone(presentationBlocks) } : {}),
    };
  }

  async update(focusId: string, input: ModelUpdateInput) {
    let focus = await this.sessions.load(focusId);
    if (input.focus) {
      const model = await this.models.load();
      const ids = [...new Set(input.focus.workstreamIds)];
      for (const id of ids) if (!model.workstreams.some((workstream) => workstream.id === id)) throw new Error(`Unknown focus workstream: ${id}`);
      focus = await this.sessions.mutate(focusId, (session) => { session.workstreamIds = ids; });
    }
    const hasSemanticInput = Boolean(input.add?.length || input.patch?.length || input.removeIds?.length || input.currentUnderstanding || input.specViews);
    if (!hasSemanticInput) return { action: "update", focusId, focus: summarizeFocus(focus), changedIds: [], generatedPaths: [], staleGeneratedPaths: [] };
    const result = await this.transact(async (draft, changed) => { applyUpdate(draft, focus, input, changed); });
    return receipt("update", result, { focusId, focus: summarizeFocus(focus) });
  }

  async recordDirection(
    focusId: string,
    input: { directions?: DirectionInput[]; currentUnderstanding?: ModelUpdateInput["currentUnderstanding"]; specViews?: SpecProjectionView[]; cutover?: { candidateManifestHash: string } },
    interactionRef?: string,
  ) {
    if (!interactionRef?.trim()) throw new Error("record_direction requires a bound interactive user turn");
    if (input.cutover) {
      if (input.directions?.length || input.currentUnderstanding || input.specViews) throw new Error("Migration cutover must be an isolated authority operation");
      return this.cutover(focusId, input.cutover.candidateManifestHash, interactionRef);
    }
    if (!((input.directions?.length ?? 0) > 0)) throw new Error("record_direction requires directions");
    const focus = await this.sessions.load(focusId);
    const batchRef = sha256({ interactionRef, directions: input.directions });
    const reconciledReviewPointIds: string[] = [];
    let activeReviewAfterDirection: FocusReview | null | undefined;
    const result = await this.transact(async (draft, changed) => {
      const beforeDirection = structuredClone(draft);
      if (input.specViews) { draft.project.projections.specs = structuredClone(input.specViews); changed.add("project.projections"); }
      const appliedTargets = input.directions!.map((direction) => {
        const object = applyDirection(draft, focus, direction, "direct_direction", interactionRef, batchRef, changed);
        return { collection: direction.collection, id: object.id };
      });
      if (input.currentUnderstanding) setCurrentUnderstanding(draft, input.currentUnderstanding, changed);

      if (focus.activeReview) {
        const directResults = new Set(appliedTargets.map(({ collection, id }) => directionResultKey(draft, collection, id)));
        for (const point of focus.activeReview.points.filter(({ purpose }) => purpose === "decision")) {
          if (!reviewPointFresh(beforeDirection, point)) continue;
          if (reviewDirections(point).some((direction) => directResults.has(previewDirectionResultKey(beforeDirection, focus, direction)))) {
            reconciledReviewPointIds.push(point.id);
          }
        }
        if (reconciledReviewPointIds.length) {
          const points = focus.activeReview.points
            .filter(({ id }) => !reconciledReviewPointIds.includes(id))
            .map((point) => refreshReviewPoint(draft, point));
          if (points.length) {
            activeReviewAfterDirection = { ...focus.activeReview, points };
            delete activeReviewAfterDirection.presentedAt;
          } else activeReviewAfterDirection = null;
        }
      }
    }, async () => {
      if (activeReviewAfterDirection === undefined) return;
      const session = structuredClone(focus);
      if (activeReviewAfterDirection) session.activeReview = activeReviewAfterDirection;
      else delete session.activeReview;
      await this.sessions.write(session);
    });
    return receipt("record_direction", result, { focusId, receiptMode: "direct_direction", batchRef, reconciledReviewPointIds });
  }

  async cutover(focusId: string, acceptedCandidateHash: string, interactionRef: string) {
    const focus = await this.sessions.load(focusId);
    const batchRef = sha256({ interactionRef, acceptedCandidateHash, operation: "migration_cutover" });
    const result = await this.transact(async (draft, changed) => {
      if (draft.project.mode !== "candidate") throw new Error("Project model is already authoritative");
      const actual = candidateManifestHash(draft);
      if (actual !== acceptedCandidateHash) throw new Error(`Candidate manifest is stale: expected ${actual}`);
      for (const { collection, object } of allObjects(draft)) {
        const acceptedState = ACCEPTED_STATE[collection];
        if (acceptedState && ["proposed", "not_reviewed", "candidate"].includes(object.state)) (object as ModelObjectBase).state = acceptedState;
        if (requiresAcceptance(collection, object.state)) {
          object.updatedAt = nowIso();
          object.acceptance = createAcceptance(collection, object, "migration_cutover", interactionRef, batchRef);
          changed.add(object.id);
        }
      }
      if (draft.project.currentUnderstanding) {
        draft.project.currentUnderstanding.sourceObjects = draft.project.currentUnderstanding.sourceObjects.map(({ id }) => {
          const found = findObject(draft, id);
          if (!found) throw new Error(`Current understanding references missing cutover object: ${id}`);
          return { id, semanticHash: semanticHash(found.collection, found.object) };
        });
        draft.project.currentUnderstanding.generatedAt = nowIso();
        changed.add("project.currentUnderstanding");
      }
      draft.project.mode = "authoritative";
      changed.add("project.mode");
    }, undefined, { replaceUnmanagedSpecs: true });
    return receipt("migration_cutover", result, { focusId: focus.id, receiptMode: "migration_cutover", acceptedCandidateHash, batchRef });
  }

  async createReview(focusId: string, input: { id?: string; key?: string; title: string; points: ReviewPointInput[] }) {
    await this.reconcileSatisfiedReview(focusId);
    const model = await this.models.load();
    const focus = await this.sessions.load(focusId);
    if (focus.activeReview) throw new Error(`Focus already has an active review: ${focus.activeReview.id}`);
    if (!(input.points?.length > 0)) throw new Error("Review requires points");
    const reservedIds = new Set(allObjects(model).map(({ object }) => object.id));
    const review: FocusReview = {
      id: normalizeNestedId("review", input.id ?? input.key ?? input.title),
      title: input.title,
      createdAt: nowIso(),
      points: input.points.map((point) => normalizeReviewPoint(model, focus, point, reservedIds)),
    };
    assertUniqueReviewIds(review);
    await this.sessions.mutate(focusId, (session) => { session.activeReview = review; });
    return {
      action: "review",
      revision: model.project.revision,
      modelHash: modelHash(model),
      focusId,
      review: projectReview(review),
      reviewHash: reviewSemanticHash(projectReview(review)),
      markdown: renderReviewMarkdown(model, review),
      next: "Present this exact review turn; resolve only after the user responds.",
    };
  }

  async markReviewPresented(focusId: string, reviewId: string, expectedReviewHash?: string) {
    const model = await this.models.load();
    const focus = await this.sessions.load(focusId);
    if (!focus.activeReview || focus.activeReview.id !== reviewId) return false;
    if (expectedReviewHash && reviewSemanticHash(projectReview(focus.activeReview)) !== expectedReviewHash) return false;
    const objects = scopedObjects(model, focus).map(({ collection, object }) => ({ id: object.id, semanticHash: semanticHash(collection, object), state: object.state }));
    await this.sessions.mutate(focusId, (session) => {
      if (!session.activeReview || session.activeReview.id !== reviewId) return;
      session.activeReview.presentedAt = nowIso();
      session.previousReview = {
        modelHash: modelHash(model),
        projectionVersion: 1,
        workstreamIds: session.workstreamIds,
        objects,
        presentedAt: session.activeReview.presentedAt,
      };
    });
    return true;
  }

  async reconcileSatisfiedReview(focusId: string) {
    const model = await this.models.load();
    const focus = await this.sessions.load(focusId);
    if (!focus.activeReview) return { reconciledReviewPointIds: [], remainingPointIds: [] };
    const reconciledReviewPointIds = focus.activeReview.points
      .filter(({ purpose }) => purpose === "decision")
      .filter((point) => reviewDirections(point).some((direction) => reviewDirectionSatisfied(model, focus, direction)))
      .map(({ id }) => id);
    if (!reconciledReviewPointIds.length) return { reconciledReviewPointIds, remainingPointIds: focus.activeReview.points.map(({ id }) => id) };

    const points = focus.activeReview.points
      .filter(({ id }) => !reconciledReviewPointIds.includes(id))
      .map((point) => refreshReviewPoint(model, point));
    const session = structuredClone(focus);
    if (points.length) {
      session.activeReview = { ...focus.activeReview, points };
      delete session.activeReview.presentedAt;
    } else delete session.activeReview;
    await this.sessions.write(session);
    return { reconciledReviewPointIds, remainingPointIds: points.map(({ id }) => id) };
  }

  async resolveReview(
    focusId: string,
    input: { reviewId?: string; outcomes?: ReviewOutcomeInput[]; update?: ModelUpdateInput; currentUnderstanding?: ModelUpdateInput["currentUnderstanding"] },
    interactionRef?: string,
  ) {
    const focus = await this.sessions.load(focusId);
    const review = focus.activeReview;
    if (!review) throw new Error("No active review");
    if (input.reviewId && input.reviewId !== review.id) throw new Error(`Active review is ${review.id}, not ${input.reviewId}`);
    if (!review.presentedAt) throw new Error("Review has not been presented successfully");
    if (!interactionRef?.trim()) throw new Error("resolve_review requires a bound interactive user turn");
    const outcomeList = input.outcomes ?? [];
    const outcomes = new Map(outcomeList.map((outcome) => [outcome.pointId, outcome]));
    if (outcomes.size !== outcomeList.length) throw new Error("Review outcomes contain duplicate point IDs");
    for (const id of outcomes.keys()) if (!review.points.some((point) => point.id === id)) throw new Error(`Review outcome references unknown point: ${id}`);
    const stalePointIds: string[] = [];
    const appliedPointIds: string[] = review.points.filter(({ purpose }) => purpose === "awareness").map(({ id }) => id);
    const batchRef = sha256({ interactionRef, reviewId: review.id, outcomes: input.outcomes ?? [] });
    let unresolvedPointIds: string[] = [];
    let remainingFocusReview: FocusReview | undefined;
    let remainingReview: { review: ReturnType<typeof projectReview>; markdown: string } | undefined;
    const result = await this.transact(async (draft, changed) => {
      for (const point of review.points.filter(({ purpose }) => purpose === "decision")) {
        const outcome = outcomes.get(point.id);
        if (!outcome || outcome.action === "unresolved") continue;
        if (!reviewPointFresh(draft, point)) { stalePointIds.push(point.id); continue; }
        if (outcome.action === "accept") {
          if (!outcome.optionId) throw new Error(`${point.id} acceptance requires optionId`);
          const option = point.options.find(({ id }) => id === outcome.optionId);
          if (!option) throw new Error(`${point.id} references unknown option ${outcome.optionId}`);
          if (!option.direction) throw new Error(`${point.id}/${option.id} has no reviewed authority payload`);
          if (outcome.direction && canonicalStringify(outcome.direction) !== canonicalStringify(option.direction)) throw new Error(`${point.id} outcome direction differs from the reviewed option`);
          applyDirection(draft, focus, option.direction, "accepted_existing", interactionRef, batchRef, changed);
        } else if (outcome.action === "modify") {
          if (!outcome.direction) throw new Error(`${point.id} modification requires the user's exact authoritative direction`);
          applyDirection(draft, focus, outcome.direction, "accepted_existing", interactionRef, batchRef, changed);
        } else if (outcome.action === "reject" || outcome.action === "defer") {
          const reviewedDirection = outcome.action === "reject" ? point.rejectDirection : point.deferDirection;
          if (!reviewedDirection) throw new Error(`${point.id} ${outcome.action} has no reviewed durable disposition`);
          if (outcome.direction && canonicalStringify(outcome.direction) !== canonicalStringify(reviewedDirection)) throw new Error(`${point.id} ${outcome.action} direction differs from the reviewed disposition`);
          applyDirection(draft, focus, reviewedDirection, "accepted_existing", interactionRef, batchRef, changed);
        } else if (outcome.direction) throw new Error(`${point.id} unresolved outcome cannot carry an authoritative direction`);
        appliedPointIds.push(point.id);
      }
      if (input.update) applyUpdate(draft, focus, input.update, changed);
      if (input.currentUnderstanding) setCurrentUnderstanding(draft, input.currentUnderstanding, changed);
      unresolvedPointIds = review.points.filter((point) => point.purpose === "decision" && !appliedPointIds.includes(point.id)).map(({ id }) => id);
      if (unresolvedPointIds.length) {
        const points = review.points.filter(({ id }) => unresolvedPointIds.includes(id)).map((point) => refreshReviewPoint(draft, point));
        const remaining = { ...review, points };
        delete remaining.presentedAt;
        remainingFocusReview = remaining;
        remainingReview = { review: projectReview(remaining), markdown: renderReviewMarkdown(draft, remaining) };
      }
    }, async () => {
      const session = structuredClone(focus);
      if (remainingFocusReview) session.activeReview = remainingFocusReview;
      else delete session.activeReview;
      await this.sessions.write(session);
    });
    return receipt("resolve_review", result, { focusId, reviewId: review.id, appliedPointIds, stalePointIds, unresolvedPointIds, ...(remainingReview ? { remainingReview } : {}) });
  }

  async specs(input: { action: "preview" | "check" | "generate"; outputDirectory?: string; replaceUnmanaged?: boolean; removeStale?: boolean }) {
    const model = await this.models.load();
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
  }

  private async transact(
    mutator: (draft: ProjectModel, changed: Set<string>) => void | Promise<void>,
    finalize?: (committed: ProjectModel) => void | Promise<void>,
    options: { replaceUnmanagedSpecs?: boolean } = {},
  ) {
    const current = await this.models.load();
    const draft = structuredClone(current);
    const changed = new Set<string>();
    await mutator(draft, changed);
    draft.project.revision += 1;
    draft.project.updatedAt = nowIso();
    assertValidProjectModel(draft);
    this.projector.render(draft);

    const paths = draft.project.mode === "authoritative" ? [...new Set([...this.projector.targetPaths(current), ...this.projector.targetPaths(draft)])] : [];
    const backups = await backupPaths(this.root, paths);
    let changedPaths: string[] = [];
    let stalePaths: string[] = [];
    let modelCommitted = false;
    try {
      if (draft.project.mode === "authoritative") {
        const generated = await this.projector.generate(draft, { replaceUnmanaged: options.replaceUnmanagedSpecs });
        changedPaths = generated.changedPaths;
        stalePaths = generated.stalePaths;
      }
      const model = await this.models.write(draft);
      modelCommitted = true;
      if (finalize) await finalize(model);
      return { model, changedIds: [...changed].sort(), changedPaths, stalePaths };
    } catch (error) {
      await restorePaths(this.root, backups);
      if (modelCommitted) await this.models.write(current);
      throw error;
    }
  }
}

function applyUpdate(model: ProjectModel, focus: FocusSession, input: ModelUpdateInput, changed: Set<string>) {
  for (const addition of input.add ?? []) {
    if (!MODEL_COLLECTIONS.includes(addition.collection)) throw new Error(`Unknown model collection: ${addition.collection}`);
    const object = normalizeNewObject(model, focus, addition);
    (model[addition.collection] as ModelObject[]).push(object);
    changed.add(object.id);
  }
  for (const patch of input.patch ?? []) {
    const found = findObject(model, patch.id);
    if (!found) throw new Error(`Unknown model object: ${patch.id}`);
    const forbidden = ["acceptance", "introducedBy", "createdAt", "updatedAt", "id"].filter((key) => key in patch.changes);
    if (forbidden.length) throw new Error(`dag_model_update cannot set authority-controlled fields: ${forbidden.join(", ")}`);
    if (found.object.acceptance && touchesSemanticFields(patch.changes)) throw new Error(`dag_model_update cannot change accepted semantic content: ${patch.id}`);
    if ("state" in patch.changes && requiresAcceptance(found.collection, String(patch.changes.state))) throw new Error(`dag_model_update cannot grant governing state: ${patch.id}`);
    Object.assign(found.object, structuredClone(patch.changes), { updatedAt: nowIso() });
    changed.add(found.object.id);
  }
  for (const id of input.removeIds ?? []) {
    const found = findObject(model, id);
    if (!found) throw new Error(`Unknown model object: ${id}`);
    if (found.object.acceptance) throw new Error(`Accepted object cannot be hard-deleted: ${id}`);
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

function normalizeNewObject(model: ProjectModel, focus: FocusSession, input: AddObjectInput, introducedBy: "agent" | "user" = "agent"): ModelObject {
  const createdAt = nowIso();
  const value = sanitizeAuthorityFields(input.value) as unknown as ModelObject;
  const id = input.id ?? allocateObjectId(model, input.collection, input.key ?? String(value.title ?? input.collection));
  if (findObject(model, id)) throw new Error(`Duplicate model object: ${id}`);
  const state = String(value.state ?? DEFAULT_STATE[input.collection]);
  if (introducedBy === "agent" && requiresAcceptance(input.collection, state)) throw new Error(`dag_model_update cannot create governing ${input.collection} state ${state}`);
  const scope = value.scope ?? (focus.workstreamIds.length ? { kind: "workstreams", workstreamIds: focus.workstreamIds } : { kind: "repository" });
  return {
    ...value,
    id,
    title: String(value.title ?? "").trim(),
    body: String(value.body ?? "").trim(),
    state,
    scope,
    introducedBy,
    sourceRefs: value.sourceRefs ?? [],
    relationships: value.relationships ?? [],
    createdAt,
    updatedAt: createdAt,
  } as ModelObject;
}

function applyDirection(
  model: ProjectModel,
  focus: FocusSession,
  input: DirectionInput,
  mode: "direct_direction" | "accepted_existing",
  interactionRef: string | undefined,
  batchRef: string,
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
    object = normalizeNewObject(model, focus, { collection: input.collection, id: input.newId, key: input.key, value: input.value as Record<string, unknown> }, "user");
    (model[input.collection] as ModelObject[]).push(object);
  }
  (object as ModelObjectBase).state = input.state ?? ACCEPTED_STATE[input.collection]!;
  object.updatedAt = nowIso();
  object.acceptance = createAcceptance(input.collection, object, mode, interactionRef, batchRef);
  changed.add(object.id);
  return object;
}

function directionResultKey(model: ProjectModel, collection: ModelCollectionName, id: string): string {
  const found = findObject(model, id);
  if (!found || found.collection !== collection) throw new Error(`Direction result not found in ${collection}: ${id}`);
  return canonicalStringify({ collection, id, semanticHash: semanticHash(collection, found.object) });
}

function previewDirectionResultKey(model: ProjectModel, focus: FocusSession, direction: DirectionInput): string {
  const preview = structuredClone(model);
  const object = applyDirection(preview, focus, direction, "accepted_existing", undefined, "preview", new Set());
  return directionResultKey(preview, direction.collection, object.id);
}

function reviewDirections(point: ReviewPoint): DirectionInput[] {
  return [
    ...point.options.map(({ direction }) => direction),
    point.rejectDirection,
    point.deferDirection,
  ].filter((direction): direction is DirectionInput => Boolean(direction));
}

function reviewDirectionSatisfied(model: ProjectModel, focus: FocusSession, direction: DirectionInput): boolean {
  const id = direction.id ?? direction.newId;
  if (!id) return false;
  const actual = findObject(model, id);
  if (!actual || actual.collection !== direction.collection) return false;
  const actualHash = semanticHash(actual.collection, actual.object);
  if (!actual.object.acceptance || actual.object.acceptance.contentHash !== actualHash) return false;

  const preview = structuredClone(model);
  if (direction.newId) {
    preview[direction.collection] = (preview[direction.collection] as ModelObject[]).filter((object) => object.id !== direction.newId) as never;
  }
  try {
    const expected = applyDirection(preview, focus, direction, "accepted_existing", undefined, "preview", new Set());
    return actualHash === semanticHash(direction.collection, expected);
  } catch {
    return false;
  }
}

function setCurrentUnderstanding(model: ProjectModel, input: { body: string; sourceObjectIds: string[] }, changed: Set<string>) {
  if (!input.body?.trim()) throw new Error("Current understanding body is required");
  const sourceObjects = [...new Set(input.sourceObjectIds ?? [])].map((id) => {
    const found = findObject(model, id);
    if (!found) throw new Error(`Current understanding references missing object: ${id}`);
    return { id, semanticHash: semanticHash(found.collection, found.object) };
  });
  model.project.currentUnderstanding = { body: input.body.trim(), generatedAt: nowIso(), sourceObjects } satisfies CurrentUnderstanding;
  changed.add("project.currentUnderstanding");
}

function normalizeReviewPoint(model: ProjectModel, focus: FocusSession, input: ReviewPointInput, reservedIds: Set<string>): ReviewPoint {
  const rejectDirection = input.rejectDirection ? materializeReviewDirection(model, focus, input.rejectDirection, reservedIds) : undefined;
  const rejectDirectionValuePatch = reviewDirectionValuePatch(input.rejectDirection);
  const deferDirection = input.deferDirection ? materializeReviewDirection(model, focus, input.deferDirection, reservedIds) : undefined;
  const deferDirectionValuePatch = reviewDirectionValuePatch(input.deferDirection);
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
      return { id, semanticHash: semanticHash(found.collection, found.object) };
    }),
    options: (input.options ?? []).map((option) => {
      if (option.objectId && !findObject(model, option.objectId)) throw new Error(`Review option references missing object: ${option.objectId}`);
      const direction = option.direction ? materializeReviewDirection(model, focus, option.direction, reservedIds) : undefined;
      const directionValuePatch = reviewDirectionValuePatch(option.direction);
      const normalized = {
        id: normalizeNestedId("option", option.id ?? option.key ?? option.label),
        label: option.label,
        description: option.description,
        ...(option.objectId ? { objectId: option.objectId } : {}),
        ...(option.recommended ? { recommended: true } : {}),
        ...(option.rationale ? { rationale: option.rationale } : {}),
        ...(direction ? { direction } : {}),
        ...(directionValuePatch !== undefined ? { directionValuePatch } : {}),
      };
      return { ...normalized, semanticHash: reviewOptionHash(model, normalized) };
    }),
    ...(rejectDirection ? { rejectDirection } : {}),
    ...(rejectDirectionValuePatch !== undefined ? { rejectDirectionValuePatch } : {}),
    ...(deferDirection ? { deferDirection } : {}),
    ...(deferDirectionValuePatch !== undefined ? { deferDirectionValuePatch } : {}),
  };
  if (point.purpose === "decision" && (!point.question || !point.options.length)) throw new Error(`${point.id} decision requires a question and options`);
  return point;
}

function reviewPointFresh(model: ProjectModel, point: ReviewPoint): boolean {
  for (const ref of point.objectRefs) {
    const found = findObject(model, ref.id);
    if (!found || semanticHash(found.collection, found.object) !== ref.semanticHash) return false;
  }
  return point.options.every((option) => reviewOptionHash(model, option) === option.semanticHash);
}

function reviewOptionHash(model: ProjectModel, option: Omit<ReviewPoint["options"][number], "semanticHash"> | ReviewPoint["options"][number]): string {
  let objectHash: string | null = null;
  if (option.objectId) {
    const found = findObject(model, option.objectId);
    if (!found) return "missing";
    objectHash = semanticHash(found.collection, found.object);
  }
  let directionTargetHash: string | null = null;
  if (option.direction?.id) {
    const target = findObject(model, option.direction.id);
    if (!target || target.collection !== option.direction.collection) return "missing";
    directionTargetHash = semanticHash(target.collection, target.object);
  }
  return sha256({ id: option.id, label: option.label, description: option.description, objectId: option.objectId ?? null, objectHash, recommended: option.recommended ?? false, rationale: option.rationale ?? null, direction: option.direction ?? null, directionTargetHash });
}

function refreshReviewPoint(model: ProjectModel, point: ReviewPoint): ReviewPoint {
  const options = point.options.map((option) => {
    const direction = option.direction ? refreshReviewDirection(model, option.direction, option.directionValuePatch) : undefined;
    const refreshed = { ...option, ...(direction ? { direction } : {}) };
    return { ...refreshed, semanticHash: reviewOptionHash(model, refreshed) };
  });
  const rejectDirection = point.rejectDirection ? refreshReviewDirection(model, point.rejectDirection, point.rejectDirectionValuePatch) : undefined;
  const deferDirection = point.deferDirection ? refreshReviewDirection(model, point.deferDirection, point.deferDirectionValuePatch) : undefined;
  return {
    ...point,
    objectRefs: point.objectRefs.map(({ id }) => {
      const found = findObject(model, id);
      if (!found) throw new Error(`Unresolved review point references removed object: ${id}`);
      return { id, semanticHash: semanticHash(found.collection, found.object) };
    }),
    options,
    ...(rejectDirection ? { rejectDirection } : {}),
    ...(deferDirection ? { deferDirection } : {}),
  };
}

function refreshReviewDirection(model: ProjectModel, direction: DirectionInput, valuePatch: Record<string, unknown> | null | undefined): DirectionInput {
  if (!direction.id) return structuredClone(direction);
  const found = findObject(model, direction.id);
  if (!found || found.collection !== direction.collection) throw new Error(`Unresolved review direction target not found in ${direction.collection}: ${direction.id}`);
  const object = structuredClone(found.object);
  if (valuePatch) Object.assign(object, sanitizeAuthorityFields(valuePatch));
  (object as ModelObjectBase).state = direction.state ?? ACCEPTED_STATE[direction.collection]!;
  const { acceptance: _acceptance, createdAt: _createdAt, id: _id, introducedBy: _introducedBy, state: _state, updatedAt: _updatedAt, ...value } = object;
  return { collection: direction.collection, id: direction.id, ...(direction.key ? { key: direction.key } : {}), state: object.state, value };
}

function reviewDirectionValuePatch(direction: DirectionInput | undefined): Record<string, unknown> | null | undefined {
  if (!direction?.id) return undefined;
  return direction.value ? sanitizeAuthorityFields(direction.value as Record<string, unknown>) : null;
}

function scopedObjects(model: ProjectModel, focus: FocusSession) {
  const selected = new Set(focus.workstreamIds);
  return allObjects(model).filter(({ object }) =>
    object.scope.kind === "repository" || object.scope.workstreamIds.some((id) => selected.has(id)),
  );
}

function scopedCounts(model: ProjectModel, focus: FocusSession) {
  const counts: Record<string, number> = {};
  for (const { collection } of scopedObjects(model, focus)) counts[collection] = (counts[collection] ?? 0) + 1;
  return counts;
}

function projectDelta(model: ProjectModel, focus: FocusSession) {
  const previous = new Map((focus.previousReview?.objects ?? []).map((object) => [object.id, object]));
  const current = scopedObjects(model, focus).map(({ collection, object }) => ({ id: object.id, collection, title: object.title, state: object.state, semanticHash: semanticHash(collection, object) }));
  const currentIds = new Set(current.map(({ id }) => id));
  return {
    baselineAvailable: Boolean(focus.previousReview),
    added: current.filter(({ id }) => !previous.has(id)),
    changed: current.filter(({ id, semanticHash: hash, state }) => {
      const prior = previous.get(id);
      return prior && (prior.semanticHash !== hash || prior.state !== state);
    }),
    retired: [...previous.values()].filter(({ id }) => !currentIds.has(id)),
  };
}

function summarizeFocus(focus: FocusSession) {
  return { id: focus.id, title: focus.title, status: focus.status, workstreamIds: focus.workstreamIds, hasBaseline: Boolean(focus.previousReview) };
}
function summarizeReview(review: FocusReview) { return { id: review.id, title: review.title, pointCount: review.points.length, presentedAt: review.presentedAt }; }
function projectReview(review: FocusReview) {
  return {
    id: review.id,
    title: review.title,
    points: review.points.map(({ rejectDirectionValuePatch: _rejectPatch, deferDirectionValuePatch: _deferPatch, ...point }) => ({
      ...point,
      options: point.options.map(({ directionValuePatch: _directionPatch, ...option }) => option),
    })),
  };
}

function renderReviewMarkdown(model: ProjectModel, review: FocusReview): string {
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

function receipt<T extends Record<string, unknown>>(action: string, result: { model: ProjectModel; changedIds: string[]; changedPaths: string[]; stalePaths: string[] }, extra: T) {
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

function materializeReviewDirection(model: ProjectModel, focus: FocusSession, input: DirectionInput, reservedIds: Set<string>): DirectionInput {
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
    object = normalizeNewObject(model, focus, { collection: input.collection, id: newId, key: input.key, value: input.value as Record<string, unknown> }, "user");
    reservedIds.add(newId);
  }
  (object as ModelObjectBase).state = input.state ?? ACCEPTED_STATE[input.collection]!;
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

function assertUniqueReviewIds(review: FocusReview) {
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
