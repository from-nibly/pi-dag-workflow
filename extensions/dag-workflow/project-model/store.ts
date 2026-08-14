import { realpathSync } from "node:fs";
import { access, lstat, readFile } from "node:fs/promises";
import { join, relative, resolve } from "node:path";
import { assertValidProjectModel, createEmptyModel, modelHash, normalizeModel, nowIso } from "./model.ts";
import { durableReplaceJson, withFileLock } from "./persistence.ts";
import { DEFAULT_PROJECT_MODEL_PATH, type ProjectModel } from "./types.ts";

export class ProjectModelStore {
  readonly root: string;
  readonly path: string;
  constructor(root: string, path = DEFAULT_PROJECT_MODEL_PATH) {
    const lexicalRoot = resolve(root);
    this.root = realpathSync(lexicalRoot);
    if (this.root !== lexicalRoot) throw new Error("Project-model repository root must not traverse symlinks");
    this.path = resolve(this.root, path);
    if (!isWithin(this.root, this.path)) throw new Error("Project-model path must stay inside the repository");
  }

  async exists(): Promise<boolean> {
    await assertNoSymlinkPath(this.root, this.path);
    try { await access(this.path); return true; } catch { return false; }
  }

  async initialize(id: string, title: string, mode: "candidate" | "authoritative" = "candidate"): Promise<ProjectModel> {
    await assertNoSymlinkPath(this.root, this.path);
    const model = createEmptyModel(id, title, mode);
    return withFileLock(this.path, async () => {
      if (await this.loadOptional()) throw new Error(`Project model already exists: ${this.path}`);
      return this.writeUnlocked(model, -1, null);
    });
  }

  async load(): Promise<ProjectModel> {
    await assertNoSymlinkPath(this.root, this.path);
    const model = await this.loadOptional();
    if (!model) throw new Error(`Project model not found: ${this.path}`);
    return model;
  }

  async write(input: ProjectModel, expectedRevision = input.project.revision - 1): Promise<ProjectModel> {
    await assertNoSymlinkPath(this.root, this.path);
    const model = normalizeAndValidate(input);
    assertExpectedRevision(expectedRevision);
    return withFileLock(this.path, async () => {
      const current = await this.loadOptional();
      return this.writeUnlocked(model, expectedRevision, current);
    });
  }

  async mutate(mutator: (draft: ProjectModel) => void | Promise<void>): Promise<{ beforeHash: string; afterHash: string; model: ProjectModel }> {
    await assertNoSymlinkPath(this.root, this.path);
    return withFileLock(this.path, async () => {
      const current = await this.loadOptional();
      if (!current) throw new Error(`Project model not found: ${this.path}`);
      const beforeHash = modelHash(current);
      const draft = structuredClone(current);
      await mutator(draft);
      draft.project.revision += 1;
      draft.project.updatedAt = nowIso();
      const model = await this.writeUnlocked(normalizeAndValidate(draft), current.project.revision, current);
      return { beforeHash, afterHash: modelHash(model), model };
    });
  }

  private async loadOptional(): Promise<ProjectModel | null> {
    let raw: string;
    try { raw = await readFile(this.path, "utf8"); }
    catch (error: any) { if (error?.code === "ENOENT") return null; throw error; }
    let parsed: ProjectModel;
    try { parsed = JSON.parse(raw) as ProjectModel; }
    catch (error: any) { throw new Error(`Malformed project model ${this.path}: ${error.message}`); }
    assertValidProjectModel(parsed);
    return normalizeModel(parsed);
  }

  private async writeUnlocked(model: ProjectModel, expectedRevision: number, current: ProjectModel | null): Promise<ProjectModel> {
    const actualRevision = current?.project.revision ?? -1;
    if (actualRevision !== expectedRevision) {
      throw new Error(`Project model revision conflict: expected ${expectedRevision}, found ${actualRevision}`);
    }
    if (model.project.revision !== expectedRevision + 1) {
      throw new Error(`Project model revision must advance from ${expectedRevision} to ${expectedRevision + 1}`);
    }
    await durableReplaceJson(this.path, model, { enableTestCrashPoints: true });
    return model;
  }
}

function normalizeAndValidate(input: ProjectModel): ProjectModel {
  const model = normalizeModel(input);
  assertValidProjectModel(model);
  if (!Number.isSafeInteger(model.project.revision) || model.project.revision < 0) throw new Error("Project model revision must be a non-negative safe integer");
  return model;
}

function assertExpectedRevision(value: number): void {
  if (!Number.isSafeInteger(value) || value < -1) throw new Error("Expected project model revision must be an integer of at least -1");
}

function isWithin(root: string, target: string): boolean {
  return target === root || target.startsWith(`${root}/`);
}

async function assertNoSymlinkPath(root: string, target: string) {
  const rel = relative(root, target);
  if (rel.startsWith("..") || rel.startsWith("/")) throw new Error("Project-model path escapes the repository");
  let current = root;
  try {
    if ((await lstat(current)).isSymbolicLink()) throw new Error(`Refusing project-model repository root symlink: ${current}`);
  } catch (error: any) { if (error?.code !== "ENOENT") throw error; }
  for (const segment of rel.split(/[\\/]/).filter(Boolean)) {
    current = join(current, segment);
    try {
      if ((await lstat(current)).isSymbolicLink()) throw new Error(`Refusing project-model path through symlink: ${current}`);
    } catch (error: any) {
      if (error?.code !== "ENOENT") throw error;
      break;
    }
  }
}
