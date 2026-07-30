import { access, lstat, mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, join, relative, resolve } from "node:path";
import { assertValidProjectModel, createEmptyModel, modelHash, normalizeModel, nowIso } from "./model.ts";
import { DEFAULT_PROJECT_MODEL_PATH, type ProjectModel } from "./types.ts";

export class ProjectModelStore {
  readonly root: string;
  readonly path: string;

  constructor(root: string, path = DEFAULT_PROJECT_MODEL_PATH) {
    this.root = resolve(root);
    this.path = resolve(this.root, path);
    if (!isWithin(this.root, this.path)) throw new Error("Project-model path must stay inside the repository");
  }

  async exists(): Promise<boolean> {
    await assertNoSymlinkPath(this.root, this.path);
    try { await access(this.path); return true; } catch { return false; }
  }

  async initialize(id: string, title: string, mode: "candidate" | "authoritative" = "candidate"): Promise<ProjectModel> {
    if (await this.exists()) throw new Error(`Project model already exists: ${this.path}`);
    const model = createEmptyModel(id, title, mode);
    await this.write(model);
    return model;
  }

  async load(): Promise<ProjectModel> {
    await assertNoSymlinkPath(this.root, this.path);
    let raw: string;
    try { raw = await readFile(this.path, "utf8"); }
    catch (error: any) {
      if (error?.code === "ENOENT") throw new Error(`Project model not found: ${this.path}`);
      throw error;
    }
    let parsed: ProjectModel;
    try { parsed = JSON.parse(raw) as ProjectModel; }
    catch (error: any) { throw new Error(`Malformed project model ${this.path}: ${error.message}`); }
    assertValidProjectModel(parsed);
    return normalizeModel(parsed);
  }

  async write(input: ProjectModel): Promise<ProjectModel> {
    await assertNoSymlinkPath(this.root, this.path);
    const model = normalizeModel(input);
    assertValidProjectModel(model);
    await mkdir(dirname(this.path), { recursive: true });
    const temporary = `${this.path}.${process.pid}.${Date.now()}.tmp`;
    await writeFile(temporary, `${JSON.stringify(model, null, 2)}\n`, "utf8");
    await rename(temporary, this.path);
    return model;
  }

  async mutate(mutator: (draft: ProjectModel) => void | Promise<void>): Promise<{ beforeHash: string; afterHash: string; model: ProjectModel }> {
    const current = await this.load();
    const beforeHash = modelHash(current);
    const draft = structuredClone(current);
    await mutator(draft);
    draft.project.revision += 1;
    draft.project.updatedAt = nowIso();
    const model = await this.write(draft);
    return { beforeHash, afterHash: modelHash(model), model };
  }
}

function isWithin(root: string, target: string): boolean {
  return target === root || target.startsWith(`${root}/`);
}

async function assertNoSymlinkPath(root: string, target: string) {
  let current = root;
  for (const segment of relative(root, target).split(/[\\/]/).filter(Boolean)) {
    current = join(current, segment);
    try {
      if ((await lstat(current)).isSymbolicLink()) throw new Error(`Refusing project-model path through symlink: ${current}`);
    } catch (error: any) {
      if (error?.code !== "ENOENT") throw error;
      break;
    }
  }
}
