import { createHash } from "node:crypto";
import { lstat, mkdir, readFile, readdir, rename, writeFile } from "node:fs/promises";
import { basename, dirname, posix, resolve } from "node:path";
import { migrateLegacyBrainstorm } from "./migration.ts";
import { candidateManifestHash, createEmptyModel, slugify } from "./model.ts";
import { SpecProjector } from "./projector.ts";
import { FocusSessionStore } from "./sessions.ts";
import { ProjectModelStore } from "./store.ts";
import type { MigrationArtifactDisposition, MigrationMetadata, MigrationSourceDisposition, ProjectModel } from "./types.ts";

const MIGRATION_FOCUS_ID = "focus-project-model-migration";
const MAX_INVENTORY_FILES = 500;
const MAX_HASH_BYTES = 5 * 1024 * 1024;
const ORIENTATION_NAMES = new Set([
  "package.json", "pyproject.toml", "cargo.toml", "go.mod", "gemfile", "composer.json", "mix.exs", "deno.json", "deno.jsonc",
]);
const EXCLUDED_DIRECTORIES = new Set([
  ".git", ".ai", "project-model", "node_modules", "vendor", "dist", "build", "target", "coverage", ".next", ".cache", ".venv", "venv",
]);

export interface MigrationMetadataInput {
  phase: MigrationMetadata["phase"];
  sources: Array<{ path: string; kind: string; disposition: MigrationSourceDisposition; reason?: string }>;
  artifacts: Array<{ path: string; disposition: MigrationArtifactDisposition; reason?: string }>;
  blockers?: string[];
}

export interface MigrationBootstrapResult {
  model: ProjectModel;
  focusId: string;
  created: boolean;
  usedLegacyAdapter: boolean;
  sourceCount: number;
  artifactCount: number;
  candidateManifestHash: string;
}

export async function bootstrapProjectMigration(rootInput: string): Promise<MigrationBootstrapResult> {
  const root = resolve(rootInput);
  const models = new ProjectModelStore(root);
  const sessions = new FocusSessionStore(root);
  let created = false;
  let usedLegacyAdapter = false;

  if (!await models.exists()) {
    const legacyPath = resolve(root, ".ai/brainstorm/structured-brainstorming.json");
    const legacy = await readJsonOptional(legacyPath);
    if (legacy) {
      const migrated = await migrateLegacyBrainstorm(root, legacy);
      await models.write(migrated.model);
      await atomicWrite(resolve(root, "project-model/migrations/brainstorm-v2-candidate.md"), migrated.report);
      await new SpecProjector(root).preview(migrated.model, ".ai/model-migration/candidate");
      usedLegacyAdapter = true;
    } else {
      const { id, title } = await inferProjectIdentity(root);
      await models.write(createEmptyModel(id, title, "candidate"));
    }
    created = true;
  }

  let model = await models.load();
  if (model.project.mode === "authoritative") throw new Error("Refusing /dag migrate because this repository already has an authoritative project model");

  if (!model.project.migration) {
    const inventory = await inventoryMigrationSources(root);
    const result = await models.mutate((draft) => {
      draft.project.migration = {
        schemaVersion: 1,
        focusId: MIGRATION_FOCUS_ID,
        phase: "inventory",
        sources: inventory.sources,
        artifacts: inventory.artifacts,
        blockers: [
          "Candidate project meaning has not been audited.",
          "Artifact dispositions and generated projection targets are incomplete.",
        ],
        updatedAt: new Date().toISOString(),
      };
    });
    model = result.model;
  }

  const focusId = model.project.migration.focusId;
  const existing = await sessions.list();
  if (existing.some(({ id }) => id === focusId)) {
    await sessions.mutate(focusId, (focus) => {
      focus.status = "active";
      focus.workstreamIds = model.workstreams.map(({ id }) => id).sort();
    });
  } else {
    await sessions.create({
      id: focusId,
      title: "Project-model migration",
      seed: "Infer and audit this repository's candidate project model before exact cutover.",
      workstreamIds: model.workstreams.map(({ id }) => id),
    });
  }

  return {
    model,
    focusId,
    created,
    usedLegacyAdapter,
    sourceCount: model.project.migration.sources.length,
    artifactCount: model.project.migration.artifacts.length,
    candidateManifestHash: candidateManifestHash(model),
  };
}

export async function materializeMigrationMetadata(rootInput: string, model: ProjectModel, input: MigrationMetadataInput): Promise<MigrationMetadata> {
  if (model.project.mode !== "candidate") throw new Error("Migration metadata can be changed only while the project model is a candidate");
  const root = resolve(rootInput);
  const rendered = new Map(new SpecProjector(root).render(model).map((file) => [file.path, hashBytes(Buffer.from(file.content))]));
  const sources = [];
  const sourcePaths = new Set<string>();
  for (const source of input.sources ?? []) {
    const path = normalizeRepositoryPath(source.path);
    if (sourcePaths.has(path)) throw new Error(`Duplicate migration source: ${path}`);
    sourcePaths.add(path);
    sources.push({
      path,
      kind: requireText(source.kind, `Migration source ${path} kind`),
      disposition: source.disposition,
      observedHash: await hashRepositoryFile(root, path, false),
      ...(source.reason?.trim() ? { reason: source.reason.trim() } : {}),
    });
  }

  const artifacts = [];
  const artifactPaths = new Set<string>();
  for (const artifact of input.artifacts ?? []) {
    const path = normalizeRepositoryPath(artifact.path);
    if (artifactPaths.has(path)) throw new Error(`Duplicate migration artifact: ${path}`);
    artifactPaths.add(path);
    artifacts.push({
      path,
      disposition: artifact.disposition,
      observedHash: await hashRepositoryFile(root, path, true),
      ...(rendered.has(path) ? { generatedHash: rendered.get(path)! } : {}),
      ...(artifact.reason?.trim() ? { reason: artifact.reason.trim() } : {}),
    });
  }

  const migration: MigrationMetadata = {
    schemaVersion: 1,
    focusId: model.project.migration?.focusId ?? MIGRATION_FOCUS_ID,
    phase: input.phase,
    sources: sources.sort((a, b) => a.path.localeCompare(b.path)),
    artifacts: artifacts.sort((a, b) => a.path.localeCompare(b.path)),
    blockers: [...new Set((input.blockers ?? []).map((value) => value.trim()).filter(Boolean))].sort(),
    updatedAt: new Date().toISOString(),
  };
  const errors = migrationReadinessErrors(model, migration);
  if (migration.phase === "ready" && errors.length) throw new Error(`Migration candidate is not cutover-ready:\n- ${errors.join("\n- ")}`);
  return migration;
}

export function migrationReadinessErrors(model: ProjectModel, migration = model.project.migration): string[] {
  if (!migration) return ["Migration metadata is missing."];
  const errors: string[] = [];
  if (migration.blockers.length) errors.push(...migration.blockers.map((blocker) => `Blocker: ${blocker}`));
  if (!migration.sources.length) errors.push("No repository sources were inventoried.");
  const sourceRefs = new Set(allSourceRefs(model));
  for (const source of migration.sources) {
    if (source.disposition === "unreviewed") errors.push(`Source is unreviewed: ${source.path}`);
    else if (!source.reason?.trim()) errors.push(`Source disposition lacks rationale: ${source.path}`);
    if (source.disposition === "mapped" && !sourceRefs.has(source.path) && !(source.kind === "legacy_model" && [...sourceRefs].some((ref) => ref.startsWith("legacy-brainstorm:")))) errors.push(`Mapped source is not referenced by a model object: ${source.path}`);
  }
  const manualLinks = new Set(model.project.projections.specs.flatMap((view) => view.manualLinks?.map(({ path }) => path) ?? []));
  for (const artifact of migration.artifacts) {
    if (artifact.disposition === "unresolved") errors.push(`Artifact disposition is unresolved: ${artifact.path}`);
    else if (!artifact.reason?.trim()) errors.push(`Artifact disposition lacks rationale: ${artifact.path}`);
    if (artifact.disposition === "block") errors.push(`Artifact blocks cutover: ${artifact.path}`);
    if (artifact.disposition === "retain_reference" && !manualLinks.has(artifact.path)) errors.push(`Retained reference is not linked by a projection: ${artifact.path}`);
    if (artifact.disposition === "retain_evidence" && !sourceRefs.has(artifact.path)) errors.push(`Retained evidence is not referenced by a model object: ${artifact.path}`);
  }
  const artifactByPath = new Map(migration.artifacts.map((artifact) => [artifact.path, artifact]));
  const projectionTargets = new SpecProjector(".").render(model).map(({ path }) => path);
  if (!projectionTargets.length) errors.push("No generated specification projections are declared.");
  for (const path of projectionTargets) {
    const artifact = artifactByPath.get(path);
    if (!artifact) errors.push(`Projection target has no artifact disposition: ${path}`);
    else if (!new Set<MigrationArtifactDisposition>(["create_generated", "replace_generated"]).has(artifact.disposition)) errors.push(`Projection target is not approved for generation: ${path}`);
  }
  const candidateAuthority = [
    ...model.intents.filter(({ state }) => state === "proposed"),
    ...model.concepts.filter(({ state }) => state === "proposed"),
    ...model.scenarios.filter(({ state }) => state === "proposed"),
    ...model.decisions.filter(({ state }) => state === "candidate"),
    ...model.commitments.filter(({ state }) => ["proposed", "not_reviewed"].includes(state)),
  ];
  if (!candidateAuthority.length) errors.push("Candidate contains no proposed governing project meaning.");
  for (const object of candidateAuthority) if (!object.sourceRefs.length) errors.push(`Proposed governing object lacks source traceability: ${object.id}`);
  if (model.questions.some(({ kind, state }) => ["contradiction", "reconsideration"].includes(kind) && state === "open")) errors.push("Open contradiction or authority reconsideration remains.");
  return [...new Set(errors)].sort();
}

export async function assertFreshMigrationReadiness(rootInput: string, model: ProjectModel): Promise<void> {
  const migration = model.project.migration;
  if (!migration || migration.phase !== "ready") throw new Error("Migration candidate is not marked cutover-ready");
  const errors = migrationReadinessErrors(model, migration);
  const root = resolve(rootInput);
  for (const source of migration.sources) {
    const current = await hashRepositoryFile(root, source.path, false).catch(() => null);
    if (current !== source.observedHash) errors.push(`Migration source changed after review: ${source.path}`);
  }
  for (const artifact of migration.artifacts) {
    const current = await hashRepositoryFile(root, artifact.path, true).catch(() => null);
    if (current !== artifact.observedHash) errors.push(`Migration artifact changed after review: ${artifact.path}`);
  }
  const rendered = new Map(new SpecProjector(root).render(model).map((file) => [file.path, hashBytes(Buffer.from(file.content))]));
  for (const artifact of migration.artifacts.filter(({ disposition }) => ["create_generated", "replace_generated"].includes(disposition))) {
    if (artifact.generatedHash !== rendered.get(artifact.path)) errors.push(`Generated projection changed after review: ${artifact.path}`);
    if (artifact.disposition === "create_generated" && artifact.observedHash !== null) errors.push(`Create target already exists: ${artifact.path}`);
    if (artifact.disposition === "replace_generated" && artifact.observedHash === null) errors.push(`Replace target no longer exists: ${artifact.path}`);
  }
  if (errors.length) throw new Error(`Migration candidate is not cutover-ready:\n- ${[...new Set(errors)].sort().join("\n- ")}`);
}

async function inventoryMigrationSources(root: string) {
  const paths: string[] = [];
  const walk = async (directory: string) => {
    if (paths.length >= MAX_INVENTORY_FILES) return;
    let entries;
    try { entries = await readdir(directory, { withFileTypes: true }); } catch { return; }
    for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
      if (paths.length >= MAX_INVENTORY_FILES) break;
      if (entry.isSymbolicLink()) continue;
      const absolute = resolve(directory, entry.name);
      const relative = posixify(absolute.slice(`${root}/`.length));
      if (entry.isDirectory()) {
        if (!EXCLUDED_DIRECTORIES.has(entry.name.toLowerCase())) await walk(absolute);
      } else if (entry.isFile() && isRelevantSourcePath(relative)) paths.push(relative);
    }
  };
  await walk(root);
  const legacyPath = ".ai/brainstorm/structured-brainstorming.json";
  if (await hashRepositoryFile(root, legacyPath, false).then(() => true, () => false)) paths.unshift(legacyPath);
  const unique = [...new Set(paths)].slice(0, MAX_INVENTORY_FILES);
  const sources = [];
  const artifacts = [];
  for (const path of unique) {
    const observedHash = await hashRepositoryFile(root, path, false);
    sources.push({ path, kind: classifySource(path), disposition: "unreviewed" as const, observedHash });
    if (path.startsWith("spec/") && path.endsWith(".md")) artifacts.push({ path, disposition: "unresolved" as const, observedHash });
  }
  return { sources, artifacts };
}

async function inferProjectIdentity(root: string): Promise<{ id: string; title: string }> {
  const packageJson = await readJsonOptional(resolve(root, "package.json"));
  const packageName = typeof packageJson?.name === "string" ? packageJson.name.trim() : "";
  if (packageName) return { id: slugify(packageName), title: packageName };
  for (const name of ["README.md", "readme.md", "README"]) {
    try {
      const text = await readFile(resolve(root, name), "utf8");
      const heading = text.match(/^#\s+(.+)$/m)?.[1]?.trim();
      if (heading) return { id: slugify(heading), title: heading };
    } catch {}
  }
  const title = basename(root);
  return { id: slugify(title), title };
}

function isRelevantSourcePath(path: string): boolean {
  const lower = path.toLowerCase();
  const name = posix.basename(lower);
  if (ORIENTATION_NAMES.has(name) || /^readme(?:\.[a-z0-9]+)?$/.test(name)) return true;
  if (lower.startsWith("spec/") || lower.startsWith("docs/") || lower.startsWith("doc/") || lower.startsWith("adr/") || lower.startsWith("adrs/") || lower.startsWith("decisions/") || lower.startsWith("plans/")) return /\.(?:md|mdx|txt|json|ya?ml|toml)$/.test(lower);
  return /(?:^|\/)(?:architecture|design|roadmap|requirements|contributing|agents)\.md$/.test(lower);
}

function classifySource(path: string): string {
  const lower = path.toLowerCase();
  if (lower === ".ai/brainstorm/structured-brainstorming.json") return "legacy_model";
  if (lower.startsWith("spec/")) return "specification";
  if (lower.startsWith("adr/") || lower.startsWith("adrs/") || lower.startsWith("decisions/")) return "decision_record";
  if (lower.startsWith("plans/") || lower.includes("roadmap")) return "plan";
  return "orientation";
}

async function hashRepositoryFile(root: string, relativePath: string, allowMissing: boolean): Promise<string | null> {
  const path = normalizeRepositoryPath(relativePath);
  const absolute = resolve(root, path);
  if (absolute !== root && !absolute.startsWith(`${root}/`)) throw new Error(`Migration path escapes repository: ${path}`);
  let current = root;
  for (const segment of path.split("/")) {
    current = resolve(current, segment);
    try {
      const info = await lstat(current);
      if (info.isSymbolicLink()) throw new Error(`Migration path traverses a symlink: ${path}`);
    } catch (error: any) {
      if (error?.code === "ENOENT" && allowMissing) return null;
      throw error;
    }
  }
  const info = await lstat(absolute);
  if (!info.isFile()) throw new Error(`Migration path is not a file: ${path}`);
  if (info.size > MAX_HASH_BYTES) throw new Error(`Migration source exceeds ${MAX_HASH_BYTES} bytes: ${path}`);
  return hashBytes(await readFile(absolute));
}

function allSourceRefs(model: ProjectModel): string[] {
  return [model.workstreams, model.intents, model.concepts, model.evidence, model.assumptions, model.questions, model.tensions, model.scenarios, model.proposals, model.decisions, model.commitments, model.discoveries]
    .flatMap((objects) => objects.flatMap(({ sourceRefs }) => sourceRefs));
}
function hashBytes(value: Buffer): string { return `sha256:${createHash("sha256").update(value).digest("hex")}`; }
function posixify(value: string): string { return value.replaceAll("\\", "/"); }
function normalizeRepositoryPath(value: string): string {
  const path = posix.normalize(posixify(String(value ?? "")).replace(/^\.\//, ""));
  if (!path || path === "." || path.startsWith("../") || path.startsWith("/") || path.includes("\0")) throw new Error(`Unsafe migration path: ${value}`);
  return path;
}
function requireText(value: unknown, label: string): string {
  if (typeof value !== "string" || !value.trim()) throw new Error(`${label} is required`);
  return value.trim();
}
async function readJsonOptional(path: string): Promise<any | null> {
  try { return JSON.parse(await readFile(path, "utf8")); }
  catch (error: any) { if (error?.code === "ENOENT") return null; throw new Error(`Invalid JSON ${path}: ${error.message}`); }
}
async function atomicWrite(path: string, content: string) {
  await mkdir(dirname(path), { recursive: true });
  const temporary = `${path}.${process.pid}.${Date.now()}.tmp`;
  await writeFile(temporary, content, "utf8");
  await rename(temporary, path);
}
