import { randomUUID } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import { access, link, lstat, mkdir, open, readFile, readdir, rename, rm } from "node:fs/promises";
import { dirname, join, relative, resolve } from "node:path";
import { canonicalizeJson } from "../dag-runtime/common.ts";
import { assertDagPlanningPlanV1, parseDagPlanningPlanV1, sealDagPlanningPlanV1 } from "./artifact.ts";
import { selectDagPlanningPlanV1, summarizeDagPlanningPlanV1 } from "./selectors.ts";
import {
  DEFAULT_DAG_PLANNING_DIRECTORY,
  type DagPlanningDecisionStateV1,
  type DagPlanningPlanSummaryV1,
  type DagPlanningPlanV1,
} from "./types.ts";

export class DagPlanningStoreBusyError extends Error {
  constructor(planId: string) {
    super(`DAG planning record ${planId} is locked by another process`);
    this.name = "DagPlanningStoreBusyError";
  }
}

export class DagPlanningStoreConflictError extends Error {
  readonly expectedRevision: number;
  readonly actualRevision: number;

  constructor(planId: string, expectedRevision: number, actualRevision: number) {
    super(`DAG planning record ${planId} revision conflict: expected ${expectedRevision}, found ${actualRevision}`);
    this.name = "DagPlanningStoreConflictError";
    this.expectedRevision = expectedRevision;
    this.actualRevision = actualRevision;
  }
}

interface LockOwnerV1 {
  pid: number;
  processStartId: string;
  acquiredAt: string;
}

export class DagPlanningStoreV1 {
  readonly repositoryRoot: string;
  readonly directory: string;
  readonly historyDirectory: string;
  readonly locksDirectory: string;

  constructor(repositoryRoot: string, directory = DEFAULT_DAG_PLANNING_DIRECTORY) {
    this.repositoryRoot = resolve(repositoryRoot);
    this.directory = resolve(this.repositoryRoot, directory);
    this.historyDirectory = join(this.directory, ".history");
    this.locksDirectory = join(this.directory, ".locks");
    if (!isWithin(this.repositoryRoot, this.directory)) throw new Error("DAG planning store must stay inside the repository");
  }

  pathFor(planId: string): string {
    assertSafePlanId(planId);
    return join(this.directory, `${planId}.json`);
  }

  revisionPathFor(planId: string, revision: number): string {
    assertSafePlanId(planId);
    assertExpectedRevision(revision);
    return join(this.historyDirectory, planId, `${revision}.json`);
  }

  async exists(planId: string): Promise<boolean> {
    const path = this.pathFor(planId);
    await assertNoSymlinkPath(this.repositoryRoot, path);
    return exists(path);
  }

  /** Read the head by default, or one exact retained revision when supplied. */
  async read(planId: string, revision?: number): Promise<DagPlanningPlanV1> {
    if (revision !== undefined) assertExpectedRevision(revision);
    const path = revision === undefined ? this.pathFor(planId) : this.revisionPathFor(planId, revision);
    await assertNoSymlinkPath(this.repositoryRoot, path);
    let text: string;
    try { text = await readFile(path, "utf8"); }
    catch (error: any) {
      if (error?.code === "ENOENT") {
        const selector = revision === undefined ? planId : `${planId}@${revision}`;
        throw new Error(`DAG planning record not found: ${selector}`);
      }
      throw error;
    }
    try {
      const plan = parseDagPlanningPlanV1(text);
      if (plan.planId !== planId || (revision !== undefined && plan.revision !== revision)) {
        throw new Error("stored identity does not match its path");
      }
      return plan;
    } catch (error: any) {
      throw new Error(`Invalid DAG planning record ${path}: ${error.message}`, { cause: error });
    }
  }

  /** List current heads only. */
  async list(): Promise<DagPlanningPlanSummaryV1[]> {
    await assertNoSymlinkPath(this.repositoryRoot, this.directory);
    if (!await exists(this.directory)) return [];
    const names = (await readdir(this.directory)).filter((name) => /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}\.json$/.test(name)).sort();
    const plans: DagPlanningPlanV1[] = [];
    for (const name of names) plans.push(await this.read(name.slice(0, -5)));
    return plans.map(summarizeDagPlanningPlanV1);
  }

  async listRevisions(planId: string): Promise<DagPlanningPlanV1[]> {
    assertSafePlanId(planId);
    const revisionsDirectory = join(this.historyDirectory, planId);
    await assertNoSymlinkPath(this.repositoryRoot, revisionsDirectory);
    if (!await exists(revisionsDirectory)) {
      return await this.exists(planId) ? [await this.read(planId)] : [];
    }
    const revisions = (await readdir(revisionsDirectory))
      .flatMap((name) => /^([1-9][0-9]*)\.json$/.exec(name)?.[1] ?? [])
      .map(Number)
      .filter(Number.isSafeInteger)
      .sort((left, right) => left - right);
    const plans: DagPlanningPlanV1[] = [];
    for (const revision of revisions) plans.push(await this.read(planId, revision));
    return plans;
  }

  async select(selector?: string): Promise<DagPlanningPlanV1> {
    const summaries = await this.list();
    const heads: DagPlanningPlanV1[] = [];
    for (const summary of summaries) heads.push(await this.read(summary.planId));
    if (selector === undefined || /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(selector)) {
      return selectDagPlanningPlanV1(heads, selector);
    }
    const plans: DagPlanningPlanV1[] = [];
    for (const summary of summaries) plans.push(...await this.listRevisions(summary.planId));
    return selectDagPlanningPlanV1(plans, selector);
  }

  async create(plan: DagPlanningPlanV1): Promise<DagPlanningPlanV1> {
    assertDagPlanningPlanV1(plan);
    if (plan.revision !== 1) throw new Error("A new DAG planning record must begin at revision 1");
    return this.withLock(plan.planId, async () => {
      const current = await this.exists(plan.planId) ? await this.read(plan.planId) : null;
      await this.removeUncommittedHistory(plan.planId, current?.revision ?? 0);
      if (current) throw new Error(`DAG planning record already exists: ${plan.planId}`);
      await this.writeSnapshot(plan);
      return structuredClone(plan);
    });
  }

  /** Mutate only static plan content while the head remains a draft. */
  async mutateDraft(
    planId: string,
    expectedRevision: number,
    mutator: (draft: DagPlanningPlanV1) => void | Promise<void>,
    now = new Date().toISOString(),
  ): Promise<{ beforeHash: string; plan: DagPlanningPlanV1 }> {
    assertExpectedRevision(expectedRevision);
    return this.withLock(planId, async () => {
      const current = await this.read(planId);
      assertCurrentRevision(planId, expectedRevision, current.revision);
      await this.removeUncommittedHistory(planId, current.revision);
      if (current.status !== "draft") throw new Error(`DAG planning record ${planId} is ${current.status}; only drafts may be mutated`);
      const draft = structuredClone(current);
      await mutator(draft);
      assertStaticMutationBoundary(current, draft);
      const { planHash: _discarded, ...content } = draft;
      const next = sealDagPlanningPlanV1({
        ...content,
        revision: current.revision + 1,
        updatedAt: now,
      });
      await this.writeSnapshot(next);
      return { beforeHash: current.planHash, plan: structuredClone(next) };
    });
  }

  /**
   * Record an approval/authorization decision under expected-revision control.
   * The callback cannot access static plan fields, and every decision is retained
   * as a strict revision without changing the static plan hash.
   */
  async mutateDecision(
    planId: string,
    expectedRevision: number,
    mutator: (decision: DagPlanningDecisionStateV1) => void | Promise<void>,
    now = new Date().toISOString(),
  ): Promise<{ beforeHash: string; plan: DagPlanningPlanV1 }> {
    assertExpectedRevision(expectedRevision);
    return this.withLock(planId, async () => {
      const current = await this.read(planId);
      assertCurrentRevision(planId, expectedRevision, current.revision);
      await this.removeUncommittedHistory(planId, current.revision);
      if (current.status === "superseded") throw new Error(`DAG planning record ${planId} is superseded; decisions are closed`);
      const decision: DagPlanningDecisionStateV1 = structuredClone({
        status: current.status,
        approval: current.approval,
        authorization: current.authorization,
      });
      await mutator(decision);
      validateDecisionTransition(current, decision);
      if (JSON.stringify(decision) === JSON.stringify({ status: current.status, approval: current.approval, authorization: current.authorization })) {
        throw new Error("Decision mutation must change approval, authorization, or status");
      }
      const { planHash: _discarded, ...content } = current;
      const next = sealDagPlanningPlanV1({
        ...content,
        ...structuredClone(decision),
        revision: current.revision + 1,
        updatedAt: now,
      });
      if (next.planHash !== current.planHash) throw new Error("Decision mutation changed static plan identity");
      await this.writeSnapshot(next);
      return { beforeHash: current.planHash, plan: structuredClone(next) };
    });
  }

  private async removeUncommittedHistory(planId: string, committedRevision: number): Promise<void> {
    const revisionsDirectory = join(this.historyDirectory, planId);
    if (!await exists(revisionsDirectory)) return;
    const names = await readdir(revisionsDirectory);
    let changed = false;
    for (const name of names) {
      const match = /^([1-9][0-9]*)\.json$/.exec(name);
      if (match && Number(match[1]) > committedRevision) {
        await rm(join(revisionsDirectory, name), { force: true });
        changed = true;
      }
    }
    if (changed) await fsyncDirectory(revisionsDirectory);
  }

  private async ensureDirectories(): Promise<void> {
    await assertNoSymlinkPath(this.repositoryRoot, this.directory);
    const directoryMissing = !await exists(this.directory);
    await mkdir(this.directory, { recursive: true, mode: 0o700 });
    await assertNoSymlinkPath(this.repositoryRoot, this.directory);
    if (directoryMissing) await fsyncDirectory(dirname(this.directory));
    for (const child of [this.historyDirectory, this.locksDirectory]) {
      const missing = !await exists(child);
      await mkdir(child, { mode: 0o700 }).catch((error: any) => {
        if (error?.code !== "EEXIST") throw error;
      });
      await assertNoSymlinkPath(this.repositoryRoot, child);
      if (missing) await fsyncDirectory(this.directory);
    }
  }

  private async writeSnapshot(plan: DagPlanningPlanV1): Promise<void> {
    assertDagPlanningPlanV1(plan);
    await this.ensureDirectories();
    const revisionsDirectory = join(this.historyDirectory, plan.planId);
    const revisionsMissing = !await exists(revisionsDirectory);
    await mkdir(revisionsDirectory, { mode: 0o700 }).catch((error: any) => {
      if (error?.code !== "EEXIST") throw error;
    });
    await assertNoSymlinkPath(this.repositoryRoot, revisionsDirectory);
    if (revisionsMissing) await fsyncDirectory(this.historyDirectory);

    const text = `${JSON.stringify(canonicalizeJson(plan), null, 2)}\n`;
    await writeImmutableFile(this.revisionPathFor(plan.planId, plan.revision), text, revisionsDirectory);

    const path = this.pathFor(plan.planId);
    await assertNoSymlinkPath(this.repositoryRoot, path);
    const temporary = join(this.directory, `.${plan.planId}.${process.pid}.${randomUUID()}.tmp`);
    const handle = await open(temporary, fsConstants.O_WRONLY | fsConstants.O_CREAT | fsConstants.O_EXCL, 0o600);
    try { await handle.writeFile(text, "utf8"); await handle.sync(); }
    finally { await handle.close(); }
    try { await rename(temporary, path); await fsyncDirectory(this.directory); }
    catch (error) { await rm(temporary, { force: true }).catch(() => undefined); throw error; }
  }

  private async withLock<T>(planId: string, operation: () => Promise<T>): Promise<T> {
    assertSafePlanId(planId);
    await this.ensureDirectories();
    const lockPath = join(this.locksDirectory, `${planId}.lock`);
    await this.acquireLock(planId, lockPath);
    try { return await operation(); }
    finally {
      await rm(lockPath, { recursive: true, force: true }).catch(() => undefined);
      await fsyncDirectory(this.locksDirectory).catch(() => undefined);
    }
  }

  private async acquireLock(planId: string, lockPath: string): Promise<void> {
    const processStartId = await processStartIdentity(process.pid)
      ?? `runtime-start:${Math.floor(Date.now() - process.uptime() * 1_000)}`;
    const owner: LockOwnerV1 = { pid: process.pid, processStartId, acquiredAt: new Date().toISOString() };

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const candidate = join(this.locksDirectory, `.${planId}.${process.pid}.${randomUUID()}.pending`);
      await mkdir(candidate, { mode: 0o700 });
      try {
        const handle = await open(join(candidate, "owner.json"), fsConstants.O_WRONLY | fsConstants.O_CREAT | fsConstants.O_EXCL, 0o600);
        try { await handle.writeFile(`${JSON.stringify(owner)}\n`, "utf8"); await handle.sync(); }
        finally { await handle.close(); }
        await fsyncDirectory(candidate);
        try {
          await rename(candidate, lockPath);
          await fsyncDirectory(this.locksDirectory);
          return;
        } catch (error: any) {
          if (!await exists(lockPath)) throw error;
        }
      } finally {
        await rm(candidate, { recursive: true, force: true }).catch(() => undefined);
      }
      if (attempt === 0 && await this.recoverAbandonedLock(lockPath)) continue;
      throw new DagPlanningStoreBusyError(planId);
    }
    throw new DagPlanningStoreBusyError(planId);
  }

  private async recoverAbandonedLock(lockPath: string): Promise<boolean> {
    let owner: LockOwnerV1;
    try {
      const lockStat = await lstat(lockPath);
      if (!lockStat.isDirectory() || lockStat.isSymbolicLink()) return false;
      const ownerPath = join(lockPath, "owner.json");
      const ownerStat = await lstat(ownerPath);
      if (!ownerStat.isFile() || ownerStat.isSymbolicLink()) return false;
      const value = JSON.parse(await readFile(ownerPath, "utf8"));
      if (!isLockOwner(value)) return false;
      owner = value;
    } catch {
      return false;
    }

    const currentStartId = await processStartIdentity(owner.pid);
    if (currentStartId !== null && (currentStartId === undefined || currentStartId === owner.processStartId)) return false;

    const abandoned = `${lockPath}.abandoned.${randomUUID()}`;
    try { await rename(lockPath, abandoned); }
    catch (error: any) {
      if (error?.code === "ENOENT") return true;
      return false;
    }
    await rm(abandoned, { recursive: true, force: true });
    await fsyncDirectory(this.locksDirectory);
    return true;
  }
}

function assertCurrentRevision(planId: string, expectedRevision: number, actualRevision: number): void {
  if (actualRevision !== expectedRevision) throw new DagPlanningStoreConflictError(planId, expectedRevision, actualRevision);
}

function assertStaticMutationBoundary(current: DagPlanningPlanV1, draft: DagPlanningPlanV1): void {
  const immutable = ["planId", "schemaVersion", "kind", "revision", "createdAt", "updatedAt", "planHash", "status", "approval", "authorization"] as const;
  for (const field of immutable) {
    if (JSON.stringify(draft[field]) !== JSON.stringify(current[field])) {
      throw new Error(`Draft mutation cannot change ${field}; use decision mutation for approval, authorization, or status`);
    }
  }
}

function validateDecisionTransition(current: DagPlanningPlanV1, next: DagPlanningDecisionStateV1): void {
  if (current.status === "draft" && next.status !== "draft" && next.status !== "ready") throw new Error("A draft decision may only keep draft status or make the plan ready");
  if (current.status === "ready" && next.status !== "ready") throw new Error("A ready plan cannot return to draft through a decision mutation");
  if (current.approval.status !== "pending" && JSON.stringify(next.approval) !== JSON.stringify(current.approval)) {
    throw new Error("A recorded approval decision is immutable");
  }
  const authorizationChanged = JSON.stringify(next.authorization) !== JSON.stringify(current.authorization);
  if (authorizationChanged && current.status !== "ready") {
    throw new Error("Authorization requires a separately retained ready-plan revision");
  }
  if (current.authorization.status === "not_authorized" && next.authorization.status === "revoked") {
    throw new Error("Authorization cannot be revoked before it is authorized");
  }
  if (current.authorization.status === "authorized" && authorizationChanged && next.authorization.status !== "revoked") {
    throw new Error("A recorded authorization may only be revoked");
  }
  if (current.authorization.status === "revoked" && authorizationChanged) {
    throw new Error("A revoked authorization decision is immutable");
  }
}

function isLockOwner(value: unknown): value is LockOwnerV1 {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const record = value as Record<string, unknown>;
  return Object.keys(record).sort().join(",") === "acquiredAt,pid,processStartId"
    && Number.isSafeInteger(record.pid) && (record.pid as number) > 0
    && typeof record.processStartId === "string" && record.processStartId.length > 0
    && typeof record.acquiredAt === "string" && Number.isFinite(Date.parse(record.acquiredAt));
}

/** null means definitely absent; undefined means start identity is unavailable. */
async function processStartIdentity(pid: number): Promise<string | null | undefined> {
  if (process.platform !== "linux") {
    try { process.kill(pid, 0); return undefined; }
    catch (error: any) { return error?.code === "ESRCH" ? null : undefined; }
  }
  try {
    const stat = await readFile(`/proc/${pid}/stat`, "utf8");
    const close = stat.lastIndexOf(")");
    if (close < 0) return undefined;
    const fields = stat.slice(close + 2).trim().split(/\s+/);
    const startTicks = fields[19];
    return startTicks && /^\d+$/.test(startTicks) ? `linux-proc:${startTicks}` : undefined;
  } catch (error: any) {
    if (error?.code === "ENOENT" || error?.code === "ESRCH") return null;
    try { process.kill(pid, 0); return undefined; }
    catch (killError: any) { return killError?.code === "ESRCH" ? null : undefined; }
  }
}

async function writeImmutableFile(path: string, text: string, parent: string): Promise<void> {
  const temporary = join(parent, `.revision.${process.pid}.${randomUUID()}.tmp`);
  const handle = await open(temporary, fsConstants.O_WRONLY | fsConstants.O_CREAT | fsConstants.O_EXCL, 0o600);
  try { await handle.writeFile(text, "utf8"); await handle.sync(); }
  finally { await handle.close(); }
  try {
    await link(temporary, path);
    await fsyncDirectory(parent);
  } catch (error: any) {
    if (error?.code !== "EEXIST") throw error;
    const existing = await readFile(path, "utf8");
    if (existing !== text) throw new Error(`Refusing to replace immutable DAG planning revision ${path}`);
  } finally {
    await rm(temporary, { force: true }).catch(() => undefined);
  }
}

function assertExpectedRevision(revision: number): void {
  if (!Number.isSafeInteger(revision) || revision < 1) throw new Error("Expected DAG planning revision must be a positive safe integer");
}

function assertSafePlanId(planId: string): void {
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(planId)) throw new Error("DAG planning plan ID is not safe for repository-local storage");
}

function isWithin(root: string, target: string): boolean {
  const path = relative(root, target);
  return path === "" || (!path.startsWith("..") && !path.startsWith("/"));
}

async function assertNoSymlinkPath(root: string, target: string): Promise<void> {
  const relativePath = relative(root, target);
  if (relativePath.startsWith("..") || relativePath.startsWith("/")) throw new Error("DAG planning path escapes the repository");
  let current = root;
  try {
    if ((await lstat(current)).isSymbolicLink()) throw new Error(`Refusing DAG planning repository root symlink: ${current}`);
  } catch (error: any) {
    if (error?.code !== "ENOENT") throw error;
  }
  for (const segment of relativePath.split(/[\\/]/).filter(Boolean)) {
    current = join(current, segment);
    try {
      if ((await lstat(current)).isSymbolicLink()) throw new Error(`Refusing DAG planning path through symlink: ${current}`);
    } catch (error: any) {
      if (error?.code !== "ENOENT") throw error;
      break;
    }
  }
}

async function fsyncDirectory(path: string): Promise<void> {
  const handle = await open(path, fsConstants.O_RDONLY);
  try { await handle.sync(); } finally { await handle.close(); }
}

async function exists(path: string): Promise<boolean> {
  try { await access(path, fsConstants.F_OK); return true; } catch { return false; }
}
