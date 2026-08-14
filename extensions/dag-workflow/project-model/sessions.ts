import { realpathSync } from "node:fs";
import { lstat, readFile, readdir } from "node:fs/promises";
import { basename, join, relative, resolve } from "node:path";
import { nowIso, slugify } from "./model.ts";
import { durableReplaceJson, withFileLock } from "./persistence.ts";
import { DEFAULT_FOCUS_SESSION_DIR, type FocusSession, type ReviewDirection } from "./types.ts";

type RevisionedFocusSession = FocusSession & { revision: number };

export class FocusSessionStore {
  readonly root: string;
  readonly dir: string;

  constructor(root: string, directory = DEFAULT_FOCUS_SESSION_DIR) {
    const lexicalRoot = resolve(root);
    this.root = realpathSync(lexicalRoot);
    if (this.root !== lexicalRoot) throw new Error("Focus-session repository root must not traverse symlinks");
    this.dir = resolve(this.root, directory);
    if (!isWithin(this.root, this.dir)) throw new Error("Focus-session directory must stay inside the repository");
  }

  path(id: string): string { return join(this.dir, `${normalizeFocusId(id)}.json`); }

  async list(): Promise<Array<{ id: string; title: string; status: FocusSession["status"]; workstreamIds: string[]; updatedAt: string }>> {
    await assertNoSymlinkPath(this.root, this.dir);
    let names: string[];
    try { names = await readdir(this.dir); } catch { return []; }
    const sessions = [];
    for (const name of names.filter((value) => value.endsWith(".json")).sort()) {
      try {
        const session = await this.load(basename(name, ".json"));
        sessions.push({ id: session.id, title: session.title, status: session.status, workstreamIds: session.workstreamIds, updatedAt: session.updatedAt });
      } catch {
        // Invalid disposable focus files are omitted from selection and recovered explicitly by id.
      }
    }
    return sessions.sort((a, b) => a.title.localeCompare(b.title) || a.id.localeCompare(b.id));
  }

  async create(input: { id?: string; title: string; seed?: string; workstreamIds?: string[] }): Promise<FocusSession> {
    if (!input.title?.trim()) throw new Error("Focus title is required");
    const id = normalizeFocusId(input.id ?? input.title);
    const path = this.path(id);
    await assertNoSymlinkPath(this.root, path);
    return withFileLock(path, async () => {
      if (await this.loadOptional(id)) throw new Error(`Focus session already exists: ${id}`);
      const createdAt = nowIso();
      const session = normalizeFocusSession({
        schemaVersion: 1,
        revision: 0,
        id,
        title: input.title.trim(),
        ...(input.seed?.trim() ? { seed: input.seed.trim() } : {}),
        workstreamIds: [...new Set(input.workstreamIds ?? [])].sort(),
        createdAt,
        updatedAt: createdAt,
        status: "active",
      } as RevisionedFocusSession);
      return this.writeUnlocked(path, session, -1, null);
    });
  }

  async load(id: string): Promise<FocusSession> {
    await assertNoSymlinkPath(this.root, this.path(id));
    const session = await this.loadOptional(id);
    if (!session) throw new Error(`Focus session not found: ${normalizeFocusId(id)}`);
    return session;
  }

  async write(input: FocusSession, expectedRevision = storedFocusRevision(input) ?? -1): Promise<FocusSession> {
    const suppliedRevision = (input as RevisionedFocusSession).revision;
    if (suppliedRevision !== undefined && (!Number.isSafeInteger(suppliedRevision) || suppliedRevision < 0)) throw new Error("Focus session revision must be a non-negative safe integer");
    assertExpectedFocusRevision(expectedRevision);
    const session = normalizeFocusSession({ ...structuredClone(input), updatedAt: nowIso() } as FocusSession);
    validateFocusSession(session);
    const path = this.path(session.id);
    await assertNoSymlinkPath(this.root, path);
    return withFileLock(path, async () => {
      const current = await this.loadOptional(session.id);
      return this.writeUnlocked(path, session, expectedRevision, current ? focusRevision(current) : null);
    });
  }

  async mutate(id: string, mutator: (session: FocusSession) => void | Promise<void>): Promise<FocusSession> {
    const path = this.path(id);
    await assertNoSymlinkPath(this.root, path);
    return withFileLock(path, async () => {
      const session = await this.loadOptional(id);
      if (!session) throw new Error(`Focus session not found: ${normalizeFocusId(id)}`);
      const expectedRevision = focusRevision(session);
      await mutator(session);
      return this.writeUnlocked(path, normalizeFocusSession(session), expectedRevision, expectedRevision);
    });
  }

  private async loadOptional(id: string): Promise<RevisionedFocusSession | null> {
    const path = this.path(id);
    let raw: string;
    try { raw = await readFile(path, "utf8"); }
    catch (error: any) { if (error?.code === "ENOENT") return null; throw error; }
    let parsed: FocusSession;
    try { parsed = JSON.parse(raw) as FocusSession; }
    catch (error: any) { throw new Error(`Malformed focus session ${path}: ${error.message}`); }
    validateFocusSession(parsed);
    return normalizeFocusSession(parsed);
  }

  private async writeUnlocked(path: string, input: RevisionedFocusSession, expectedRevision: number, currentRevision: number | null): Promise<RevisionedFocusSession> {
    const actualRevision = currentRevision ?? -1;
    if (actualRevision !== expectedRevision) throw new Error(`Focus session revision conflict: expected ${expectedRevision}, found ${actualRevision}`);
    const session = normalizeFocusSession({ ...structuredClone(input), revision: expectedRevision + 1, updatedAt: nowIso() } as RevisionedFocusSession);
    validateFocusSession(session);
    await durableReplaceJson(path, session, { enableTestCrashPoints: true });
    return session;
  }
}

export function normalizeFocusId(value: string): string {
  const slug = slugify(value);
  return slug.startsWith("focus-") ? slug : `focus-${slug}`;
}

export function validateFocusSession(session: FocusSession): void {
  const errors: string[] = [];
  if (!session || typeof session !== "object") throw new Error("Invalid focus session:\n- session must be an object");
  if (session.schemaVersion !== 1) errors.push("schemaVersion must be 1");
  const revision = (session as RevisionedFocusSession).revision;
  if (revision !== undefined && (!Number.isSafeInteger(revision) || revision < 0)) errors.push("revision must be a non-negative safe integer");
  if (typeof session.id !== "string" || !session.id.startsWith("focus-")) errors.push("id must start with focus-");
  if (typeof session.title !== "string" || !session.title.trim()) errors.push("title is required");
  if (!Array.isArray(session.workstreamIds) || session.workstreamIds.some((id) => typeof id !== "string")) errors.push("workstreamIds must be a string array");
  if (!new Set(["active", "suspended"]).has(session.status)) errors.push("status is invalid");
  if (typeof session.createdAt !== "string" || Number.isNaN(Date.parse(session.createdAt)) || typeof session.updatedAt !== "string" || Number.isNaN(Date.parse(session.updatedAt))) errors.push("createdAt/updatedAt are invalid");
  if (session.previousReview) {
    if (typeof session.previousReview.modelHash !== "string" || !Number.isInteger(session.previousReview.projectionVersion)) errors.push("previousReview modelHash/projectionVersion is invalid");
    if (!Array.isArray(session.previousReview.workstreamIds) || session.previousReview.workstreamIds.some((id) => typeof id !== "string")) errors.push("previousReview.workstreamIds must be a string array");
    if (!Array.isArray(session.previousReview.objects) || session.previousReview.objects.some((object) => typeof object?.id !== "string" || typeof object?.semanticHash !== "string" || typeof object?.state !== "string")) errors.push("previousReview.objects is invalid");
    if (typeof session.previousReview.presentedAt !== "string" || Number.isNaN(Date.parse(session.previousReview.presentedAt))) errors.push("previousReview.presentedAt is invalid");
  }
  if (session.activeReview) {
    if (typeof session.activeReview.id !== "string" || !session.activeReview.id.startsWith("review-")) errors.push("activeReview.id must start with review-");
    const points = Array.isArray(session.activeReview.points) ? session.activeReview.points : [];
    if (!points.length) errors.push("activeReview requires points");
    const pointIds = new Set<string>();
    for (const point of points) {
      if (!point || typeof point !== "object") { errors.push("activeReview point must be an object"); continue; }
      if (typeof point.id !== "string" || !point.id.startsWith("point-")) errors.push(`review point id is invalid: ${point.id}`);
      else if (pointIds.has(point.id)) errors.push(`duplicate review point id: ${point.id}`);
      else pointIds.add(point.id);
      if (typeof point.title !== "string" || !point.title.trim() || typeof point.context !== "string" || !point.context.trim()) errors.push(`${point.id} requires title/context`);
      if (!new Set(["awareness", "decision"]).has(point.purpose)) errors.push(`${point.id} purpose is invalid`);
      if (!Array.isArray(point.objectRefs) || point.objectRefs.some((ref) => typeof ref?.id !== "string" || typeof ref?.semanticHash !== "string")) errors.push(`${point.id}.objectRefs is invalid`);
      const options = Array.isArray(point.options) ? point.options : [];
      if (point.purpose === "decision" && (typeof point.question !== "string" || !point.question.trim())) errors.push(`${point.id} requires a question`);
      if (point.purpose === "decision" && !options.length) errors.push(`${point.id} requires options`);
      const optionIds = new Set<string>();
      for (const option of options) {
        if (typeof option?.id !== "string" || !option.id.startsWith("option-") || typeof option.label !== "string" || !option.label.trim() || typeof option.description !== "string" || !option.description.trim() || typeof option.semanticHash !== "string") errors.push(`${point.id} has invalid option`);
        else if (optionIds.has(option.id)) errors.push(`${point.id} has duplicate option ${option.id}`);
        else optionIds.add(option.id);
        if (option?.direction) validateReviewDirectionShape(option.direction, `${point.id}.${option.id}.direction`, errors);
        validateDirectionValuePatch(option?.directionValuePatch, `${point.id}.${option?.id}.directionValuePatch`, errors);
      }
      if (point.rejectDirection) validateReviewDirectionShape(point.rejectDirection, `${point.id}.rejectDirection`, errors);
      validateDirectionValuePatch(point.rejectDirectionValuePatch, `${point.id}.rejectDirectionValuePatch`, errors);
      if (point.deferDirection) validateReviewDirectionShape(point.deferDirection, `${point.id}.deferDirection`, errors);
      validateDirectionValuePatch(point.deferDirectionValuePatch, `${point.id}.deferDirectionValuePatch`, errors);
    }
  }
  if (errors.length) throw new Error(`Invalid focus session:\n- ${errors.join("\n- ")}`);
}

function validateDirectionValuePatch(value: unknown, label: string, errors: string[]) {
  if (value === undefined || value === null) return;
  if (typeof value !== "object" || Array.isArray(value)) { errors.push(`${label} must be an object or null`); return; }
  const forbidden = ["acceptance", "introducedBy", "createdAt", "updatedAt", "id"].filter((key) => key in value);
  if (forbidden.length) errors.push(`${label} contains controlled fields: ${forbidden.join(", ")}`);
}

function validateReviewDirectionShape(direction: ReviewDirection, label: string, errors: string[]) {
  if (!direction || typeof direction !== "object") { errors.push(`${label} must be an object`); return; }
  if (!new Set(["intents", "concepts", "scenarios", "decisions", "commitments"]).has(direction.collection)) errors.push(`${label}.collection is invalid`);
  if (direction.id !== undefined && typeof direction.id !== "string") errors.push(`${label}.id must be a string`);
  if (direction.newId !== undefined && typeof direction.newId !== "string") errors.push(`${label}.newId must be a string`);
  if (direction.id && direction.newId) errors.push(`${label} cannot contain id and newId`);
  if (typeof direction.state !== "string" || !direction.state) errors.push(`${label}.state is required`);
  if (!direction.value || typeof direction.value !== "object" || Array.isArray(direction.value)) { errors.push(`${label}.value must be an object`); return; }
  const value = direction.value as Record<string, any>;
  if (typeof value.title !== "string" || typeof value.body !== "string") errors.push(`${label}.value title/body are required`);
  if (!value.scope || !new Set(["repository", "workstreams"]).has(value.scope.kind)) errors.push(`${label}.value.scope is invalid`);
  else if (value.scope.kind === "workstreams" && (!Array.isArray(value.scope.workstreamIds) || value.scope.workstreamIds.some((id: unknown) => typeof id !== "string"))) errors.push(`${label}.value.scope.workstreamIds must be a string array`);
  if (!Array.isArray(value.sourceRefs) || value.sourceRefs.some((ref: unknown) => typeof ref !== "string")) errors.push(`${label}.value.sourceRefs must be a string array`);
  if (!Array.isArray(value.relationships) || value.relationships.some((relation: any) => !relation || typeof relation.kind !== "string" || typeof relation.targetId !== "string")) errors.push(`${label}.value.relationships is invalid`);
}

function normalizeFocusSession(input: FocusSession): RevisionedFocusSession {
  const session = structuredClone(input) as RevisionedFocusSession;
  session.revision = storedFocusRevision(session) ?? 0;
  session.id = normalizeFocusId(session.id);
  session.workstreamIds = [...new Set(session.workstreamIds ?? [])].sort();
  if (session.previousReview) {
    session.previousReview.workstreamIds = [...new Set(session.previousReview.workstreamIds ?? [])].sort();
    session.previousReview.objects = [...(session.previousReview.objects ?? [])].sort((a, b) => a.id.localeCompare(b.id));
  }
  return session;
}

function storedFocusRevision(session: FocusSession): number | undefined {
  const revision = (session as RevisionedFocusSession).revision;
  return Number.isSafeInteger(revision) && revision >= 0 ? revision : undefined;
}

function focusRevision(session: FocusSession): number {
  const revision = storedFocusRevision(session);
  if (revision === undefined) throw new Error("Focus session requires an expected integer revision");
  return revision;
}

function assertExpectedFocusRevision(revision: number): void {
  if (!Number.isSafeInteger(revision) || revision < -1) throw new Error("Expected focus session revision must be an integer of at least -1");
}

function isWithin(root: string, target: string): boolean {
  return target === root || target.startsWith(`${root}/`);
}

async function assertNoSymlinkPath(root: string, target: string): Promise<void> {
  const rel = relative(root, target);
  if (rel.startsWith("..") || rel.startsWith("/")) throw new Error("Focus-session path escapes the repository");
  let current = root;
  try {
    if ((await lstat(current)).isSymbolicLink()) throw new Error(`Refusing focus-session repository root symlink: ${current}`);
  } catch (error: any) { if (error?.code !== "ENOENT") throw error; }
  for (const segment of rel.split(/[\\/]/).filter(Boolean)) {
    current = join(current, segment);
    try {
      if ((await lstat(current)).isSymbolicLink()) throw new Error(`Refusing focus-session path through symlink: ${current}`);
    } catch (error: any) {
      if (error?.code !== "ENOENT") throw error;
      break;
    }
  }
}
