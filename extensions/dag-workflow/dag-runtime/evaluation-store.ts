import { randomUUID } from "node:crypto";
import { constants } from "node:fs";
import { lstat, mkdir, open, readFile, readdir, realpath, rename, rmdir, unlink, link } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { Type, type Static } from "typebox";
import {
  HashSchema,
  NonNegativeIntegerSchema,
  StrictObject,
  TimestampSchema,
  canonicalHash,
  canonicalStringify,
  hashWithoutField,
  parseStrictJson,
  schemaIssues,
} from "./common.ts";
import {
  RunEvaluationEnvelopeV1Schema,
  RunObservationAccumulatorV1Schema,
  accumulatorClockV1,
  accumulatorDerivedMetricsV1,
  assertAccumulatorObservationV1,
  foldAccumulatorObservationV1,
  parseRfc3339UtcNanosecondsV1,
  runEvaluationEnvelopeHashV1,
  validateRunEvaluationEnvelopeV1,
  validateRunObservationAccumulatorV1,
  type AccumulatorObservationV1,
  type RunEvaluationEnvelopeV1,
  type RunObservationAccumulatorV1,
} from "./evaluation.ts";

const MAX_PENDING_ACCUMULATOR_CLEANUPS = 50_000;
const LOCK_CLAIM_LIMIT = 128;
const OBSERVER_RUN_ACCOUNTING_LIMIT = 128;
const LOCK_CLAIM_PREFIX = "telemetry.lock.claim.";
const HASH_PATTERN = /^sha256:[0-9a-f]{64}$/;

const EvaluationIndexEntryV1Schema = StrictObject({
  cutoffIdentityHash: HashSchema,
  envelopeHash: HashSchema,
  projectIdentityHash: HashSchema,
  runIdentityHash: HashSchema,
  runNonceHash: HashSchema,
  planHash: HashSchema,
  evaluationProfileHash: HashSchema,
  sourceRevision: NonNegativeIntegerSchema,
  sourceSnapshotHash: HashSchema,
  expectedAccumulatorHash: HashSchema,
  creditContextHash: HashSchema,
  cutoffAt: TimestampSchema,
  cutoffKind: Type.Enum(["terminal", "right_censored"]),
  indexedAt: TimestampSchema,
});
const PendingAccumulatorCleanupV1Schema = StrictObject({
  receiptHash: HashSchema,
  terminalEnvelopeHash: HashSchema,
  cutoffIdentityHash: HashSchema,
  cutoffKind: Type.Literal("terminal"),
  cutoffAt: TimestampSchema,
  projectIdentityHash: HashSchema,
  runIdentityHash: HashSchema,
  runNonceHash: HashSchema,
  planHash: HashSchema,
  evaluationProfileHash: HashSchema,
  sourceRevision: NonNegativeIntegerSchema,
  sourceSnapshotHash: HashSchema,
  expectedAccumulatorHash: HashSchema,
  creditContextHash: HashSchema,
  indexedAt: TimestampSchema,
});
export const RunEvaluationIndexV1Schema = StrictObject({
  schemaVersion: Type.Literal(1),
  kind: Type.Literal("run_evaluation_index"),
  canonicalization: Type.Literal("jcs-v1"),
  entries: Type.Array(EvaluationIndexEntryV1Schema, { maxItems: 50_000 }),
  pendingAccumulatorCleanup: Type.Array(PendingAccumulatorCleanupV1Schema, { maxItems: MAX_PENDING_ACCUMULATOR_CLEANUPS }),
  indexHash: HashSchema,
});
export type RunEvaluationIndexV1 = Static<typeof RunEvaluationIndexV1Schema>;
type EvaluationIndexEntryV1 = Static<typeof EvaluationIndexEntryV1Schema>;
type PendingAccumulatorCleanupV1 = Static<typeof PendingAccumulatorCleanupV1Schema>;

export interface RunEvaluationStoreOptionsV1 {
  failpoint?: (point: "after_accumulator_temp_sync" | "after_accumulator_rename" | "after_envelope_temp_sync" | "after_envelope_publish" | "before_envelope_index_commit" | "after_index_temp_sync" | "after_index_rename") => Promise<void> | void;
}
export interface RunEvaluationMaintenanceResultV1 {
  prunedEnvelopeCount: number;
  deletedAccumulatorCount: number;
  cleanupErrorCount: number;
}
const RunEvaluationObserverIdentityV1Schema = StrictObject({
  projectIdentityHash: HashSchema,
  runIdentityHash: HashSchema,
  runNonceHash: HashSchema,
  planHash: HashSchema,
  evaluationProfileHash: HashSchema,
  clockPolicyHash: HashSchema,
  creditContextHash: HashSchema,
});
export interface RunEvaluationObserverIdentityV1 {
  projectIdentityHash: string;
  runIdentityHash: string;
  runNonceHash: string;
  planHash: string;
  evaluationProfileHash: string;
  clockPolicyHash: string;
  creditContextHash: string;
}
const MinimalCommittedSnapshotIdentityV1Schema = StrictObject({
  runIdentityHash: HashSchema,
  revision: NonNegativeIntegerSchema,
  snapshotHash: HashSchema,
});
const BoundCommittedSnapshotIdentityV1Schema = StrictObject({
  runIdentityHash: HashSchema,
  revision: NonNegativeIntegerSchema,
  snapshotHash: HashSchema,
  projectIdentityHash: HashSchema,
  runNonceHash: HashSchema,
  planHash: HashSchema,
  evaluationProfileHash: HashSchema,
  clockPolicyHash: HashSchema,
  creditContextHash: HashSchema,
});
export interface CommittedSnapshotIdentityV1 {
  runIdentityHash: string;
  revision: number;
  snapshotHash: string;
  projectIdentityHash?: string;
  runNonceHash?: string;
  planHash?: string;
  evaluationProfileHash?: string;
  clockPolicyHash?: string;
  creditContextHash?: string;
}
export interface RunEvaluationFoldResultV1 {
  accumulator: RunObservationAccumulatorV1 | null;
  folded: boolean;
}
interface PathIdentity { device: string; inode: string; }
interface StorePathBindings {
  project: PathIdentity;
  ai: PathIdentity;
  root: PathIdentity;
  accumulators: PathIdentity;
  envelopes: PathIdentity;
  rootIdentityHash: string;
}
interface LockOwnerV1 {
  schemaVersion: 1;
  kind: "run_evaluation_store_lock_owner";
  pid: number;
  processStartIdentity: string;
  ownerTokenHash: string;
  rootIdentityHash: string;
  lockDirectoryIdentityHash: string;
  metadataHash: string;
}
interface OwnedLock { identity: PathIdentity; owner: LockOwnerV1; }

export class RunEvaluationStoreBusyError extends Error {
  constructor() { super("DAG_EVALUATION_STORE_BUSY"); this.name = "RunEvaluationStoreBusyError"; }
}
export class RunEvaluationStoreConflictError extends Error {
  constructor(message: string) { super(message); this.name = "RunEvaluationStoreConflictError"; }
}
export class RunEvaluationStoreCorruptError extends Error {
  readonly causeValue: unknown;
  constructor(message: string, causeValue?: unknown) { super(message); this.name = "RunEvaluationStoreCorruptError"; this.causeValue = causeValue; }
}

export class RunEvaluationStoreV1 {
  readonly projectDirectory: string;
  readonly aiDirectory: string;
  readonly rootDirectory: string;
  readonly accumulatorsDirectory: string;
  readonly envelopesDirectory: string;
  readonly indexPath: string;
  readonly lockDirectory: string;
  readonly options: RunEvaluationStoreOptionsV1;
  private bindings: StorePathBindings | null = null;

  constructor(projectRoot: string, options: RunEvaluationStoreOptionsV1 = {}) {
    this.projectDirectory = resolve(projectRoot);
    this.aiDirectory = join(this.projectDirectory, ".ai");
    this.rootDirectory = join(this.aiDirectory, "dag-evaluations-v1");
    this.accumulatorsDirectory = join(this.rootDirectory, "accumulators");
    this.envelopesDirectory = join(this.rootDirectory, "envelopes");
    this.indexPath = join(this.rootDirectory, "index.json");
    this.lockDirectory = join(this.rootDirectory, "telemetry.lock");
    this.options = options;
  }

  async initialize(): Promise<void> {
    await this.prepareAndBindDirectories();
    if (!(await this.safeFileExists(this.indexPath, "evaluation index"))) await this.withLock(async () => {
      if (!(await this.safeFileExists(this.indexPath, "evaluation index"))) await this.writeIndex(sealIndex([], []), null);
    });
  }

  async readAccumulator(runIdentityHash: string): Promise<RunObservationAccumulatorV1 | null> {
    assertHash(runIdentityHash, "runIdentityHash");
    await this.assertStoreDirectories();
    const path = this.accumulatorPath(runIdentityHash);
    let raw: string;
    try { raw = await this.readRegularFileNoFollow(path, "evaluation accumulator"); }
    catch (error: any) {
      if (error?.code === "ENOENT") return null;
      if (error instanceof RunEvaluationStoreCorruptError) throw error;
      throw new RunEvaluationStoreCorruptError(`Cannot read evaluation accumulator for ${runIdentityHash}`, error);
    }
    try {
      const value = parseStrictJson(raw);
      const validation = validateRunObservationAccumulatorV1(value);
      if (!validation.ok || canonicalStringify(value) !== raw) throw new Error(validation.issues.map(({ path, message }) => `${path} ${message}`).join("; ") || "bytes are not canonical");
      if (validation.value!.identity.runIdentityHash !== runIdentityHash) throw new Error("accumulator run identity does not match namespace");
      return validation.value!;
    } catch (error) { throw new RunEvaluationStoreCorruptError(`Invalid evaluation accumulator for ${runIdentityHash}`, error); }
  }

  async writeAccumulator(accumulator: RunObservationAccumulatorV1, expectedAccumulatorHash: string | null): Promise<RunObservationAccumulatorV1> {
    const validation = validateRunObservationAccumulatorV1(accumulator);
    if (!validation.ok) throw new Error(`Invalid accumulator: ${validation.issues.map(({ path, message }) => `${path} ${message}`).join("; ")}`);
    assertHash(accumulator.identity.runIdentityHash, "runIdentityHash");
    if (expectedAccumulatorHash !== null) assertHash(expectedAccumulatorHash, "expectedAccumulatorHash");
    await this.ensureDirectories();
    return this.withLock(async () => {
      const current = await this.readAccumulator(accumulator.identity.runIdentityHash);
      if ((current?.accumulatorHash ?? null) !== expectedAccumulatorHash) throw new RunEvaluationStoreConflictError("Accumulator expected identity does not match current head");
      if (current && accumulator.source.revision < current.source.revision) throw new RunEvaluationStoreConflictError("Accumulator source revision cannot regress");
      if (current && canonicalStringify(accumulatorImmutableIdentity(accumulator)) !== canonicalStringify(accumulatorImmutableIdentity(current))) throw new RunEvaluationStoreConflictError("Accumulator immutable identity tuple cannot change");
      if (current && accumulator.source.revision === current.source.revision) {
        if (accumulator.accumulatorHash === current.accumulatorHash) return current;
        if (canonicalStringify(accumulator.source) !== canonicalStringify(current.source)) throw new RunEvaluationStoreConflictError("Equal accumulator revision must preserve the exact source snapshot");
        throw new RunEvaluationStoreConflictError("Equal accumulator revision may only be an exact no-op");
      }
      await this.atomicReplace(this.accumulatorPath(accumulator.identity.runIdentityHash), canonicalStringify(accumulator), "accumulator", current?.accumulatorHash ?? null);
      return accumulator;
    });
  }

  async foldObservation(runIdentityHash: string, observation: AccumulatorObservationV1): Promise<RunEvaluationFoldResultV1> {
    assertHash(runIdentityHash, "runIdentityHash");
    assertAccumulatorObservationV1(observation);
    await this.ensureDirectories();
    return this.withLock(async () => {
      const current = await this.readAccumulator(runIdentityHash);
      if (!current) return { accumulator: null, folded: false };
      const observationCreditContextHash = canonicalHash(observation.creditContext);
      if (canonicalStringify(observation.creditContext) !== canonicalStringify(current.creditContext) || observationCreditContextHash !== current.creditContextHash) {
        throw new RunEvaluationStoreConflictError("Observation credit context canonical bytes and hash must equal the accumulator's immutable v1 credit context");
      }
      if (observation.revision < current.source.revision) return { accumulator: current, folded: false };
      if (observation.revision === current.source.revision) {
        const observationSource = {
          revision: observation.revision,
          snapshotHash: observation.snapshotHash,
          observedAt: observation.observedAt,
          clockEpochHash: observation.clockEpochHash,
          monotonicTickMs: observation.monotonicTickMs,
        };
        if (canonicalStringify(observationSource) !== canonicalStringify(current.source)) throw new RunEvaluationStoreConflictError("Equal observation revision must preserve the exact source snapshot");
        return { accumulator: current, folded: false };
      }
      const next = foldAccumulatorObservationV1(current, observation);
      await this.atomicReplace(this.accumulatorPath(runIdentityHash), canonicalStringify(next), "accumulator", current.accumulatorHash);
      return { accumulator: next, folded: true };
    });
  }

  async readEnvelope(envelopeHash: string): Promise<RunEvaluationEnvelopeV1> {
    assertHash(envelopeHash, "envelopeHash");
    await this.assertStoreDirectories();
    try {
      const raw = await this.readRegularFileNoFollow(this.envelopePath(envelopeHash), "evaluation envelope");
      const value = parseStrictJson(raw);
      const validation = validateRunEvaluationEnvelopeV1(value);
      if (!validation.ok || canonicalStringify(value) !== raw || validation.value!.envelopeHash !== envelopeHash) throw new Error(validation.issues.map(({ path, message }) => `${path} ${message}`).join("; ") || "envelope bytes or namespace do not match");
      return validation.value!;
    } catch (error) { throw new RunEvaluationStoreCorruptError(`Invalid evaluation envelope ${envelopeHash}`, error); }
  }

  async readIndex(): Promise<RunEvaluationIndexV1> {
    await this.assertStoreDirectories();
    try {
      const raw = await this.readRegularFileNoFollow(this.indexPath, "evaluation index");
      const value = parseStrictJson(raw);
      const validation = validateIndex(value);
      if (validation.length || canonicalStringify(value) !== raw) throw new Error(validation.map(({ path, message }) => `${path} ${message}`).join("; ") || "index bytes are not canonical");
      return value as RunEvaluationIndexV1;
    } catch (error) { throw new RunEvaluationStoreCorruptError("Invalid evaluation index", error); }
  }

  async publishEnvelope(envelope: RunEvaluationEnvelopeV1, indexedAt: string, expectedPriorEnvelopeHash: string | null = envelope.supersedesEnvelopeHash): Promise<{ index: RunEvaluationIndexV1; prunedEnvelopeCount: number }> {
    const validation = validateRunEvaluationEnvelopeV1(envelope);
    if (!validation.ok) throw new Error(`Invalid envelope: ${validation.issues.map(({ path, message }) => `${path} ${message}`).join("; ")}`);
    if (parseRfc3339UtcNanosecondsV1(indexedAt) === null) throw new Error("indexedAt must be a real UTC RFC 3339 timestamp");
    await this.ensureDirectories();
    return this.withLock(async () => {
      const sourceAccumulator = await this.readAccumulator(envelope.identity.runIdentityHash);
      if (!sourceAccumulator) throw new RunEvaluationStoreConflictError("Envelope publication requires the exact source accumulator");
      if (!accumulatorMatchesEnvelope(sourceAccumulator, envelope)) throw new RunEvaluationStoreConflictError("Envelope does not bind the exact current source accumulator");
      const exactCoverage = {
        sourceRevisionCount: sourceAccumulator.source.revision + 1,
        observedRevisionCount: sourceAccumulator.coverage.observedRevisionCount,
        missingRevisionCount: sourceAccumulator.coverage.missingRevisionCount,
        droppedRevisionCount: sourceAccumulator.coverage.droppedRevisionCount,
        censoredIntervalCount: sourceAccumulator.coverage.censoredIntervalCount,
        observerFailureCount: sourceAccumulator.coverage.observerFailureCount,
      };
      for (const [name, exact] of Object.entries(exactCoverage)) {
        if (envelope.coverage[name as keyof typeof exactCoverage] !== exact) throw new RunEvaluationStoreConflictError(`Envelope coverage ${name} does not match the exact source accumulator`);
      }
      const exactClock = accumulatorClockV1(sourceAccumulator);
      if (canonicalStringify(envelope.clock) !== canonicalStringify(exactClock)) throw new RunEvaluationStoreConflictError("Envelope clock quality does not match the exact source accumulator clock history");
      const exactMetrics = accumulatorDerivedMetricsV1(sourceAccumulator, {
        serialPolicy: envelope.serialPolicy,
        rightCensored: envelope.cutoff.kind === "right_censored",
      });
      const publishedMetrics = {
        accumulatorTelemetry: envelope.metrics.accumulatorTelemetry,
        timing: envelope.metrics.timing,
        waitHistograms: envelope.metrics.waitHistograms,
        usefulParallelism: envelope.metrics.usefulParallelism,
        humanAttention: {
          activeMinutes: envelope.metrics.humanAttention.activeMinutes,
          activeIntervals: envelope.metrics.humanAttention.activeIntervals,
          authorityWait: envelope.metrics.humanAttention.authorityWait,
          authorityWaitIntervals: envelope.metrics.humanAttention.authorityWaitIntervals,
        },
        findings: {
          falseIndependenceIncidents: envelope.metrics.findings.falseIndependenceIncidents,
          falseIndependenceWaste: envelope.metrics.findings.falseIndependenceWaste,
        },
        instrumentation: envelope.metrics.instrumentation,
      };
      if (canonicalStringify(publishedMetrics) !== canonicalStringify(exactMetrics)) throw new RunEvaluationStoreConflictError("Envelope accumulator-derived metrics do not match the exact source accumulator derivation");
      const index = await this.readIndex();
      const existing = index.entries.find(({ cutoffIdentityHash }) => cutoffIdentityHash === envelope.cutoff.cutoffIdentityHash);
      if (existing?.envelopeHash === envelope.envelopeHash) {
        await this.publishImmutableEnvelope(envelope);
        await this.verifyImmutableEnvelope(envelope);
        return { index, prunedEnvelopeCount: 0 };
      }
      if (existing) {
        if (expectedPriorEnvelopeHash !== existing.envelopeHash || envelope.supersedesEnvelopeHash !== existing.envelopeHash) throw new RunEvaluationStoreConflictError("Correction must supersede the exact current envelope head");
      } else if (expectedPriorEnvelopeHash !== null || envelope.supersedesEnvelopeHash !== null) {
        throw new RunEvaluationStoreConflictError("A new cutoff cannot supersede a missing envelope head");
      }
      const entry = entryFromEnvelope(envelope, indexedAt);
      let entries = index.entries.filter(({ cutoffIdentityHash }) => cutoffIdentityHash !== entry.cutoffIdentityHash);
      entries.push(entry);
      entries.sort(compareIndexIdentity);
      const removed: EvaluationIndexEntryV1[] = [];
      for (const projectIdentityHash of new Set(entries.map(({ projectIdentityHash }) => projectIdentityHash))) {
        const projectEntries = entries.filter((item) => item.projectIdentityHash === projectIdentityHash).sort(compareRetentionIdentity);
        if (projectEntries.length > 50) removed.push(...projectEntries.slice(0, projectEntries.length - 50));
      }
      const removedHashes = new Set(removed.map(({ envelopeHash }) => envelopeHash));
      entries = entries.filter(({ envelopeHash }) => !removedHashes.has(envelopeHash));
      const pending = [...index.pendingAccumulatorCleanup];
      for (const removedEntry of removed) {
        if (removedEntry.cutoffKind !== "terminal") continue;
        try {
          const accumulator = await this.readAccumulator(removedEntry.runIdentityHash);
          if (!accumulator) continue;
          const removedEnvelope = await this.readEnvelope(removedEntry.envelopeHash);
          if (!envelopeMatchesEntry(removedEnvelope, removedEntry) || !accumulatorMatchesEnvelope(accumulator, removedEnvelope)) continue;
          const receipt = sealCleanupReceipt(removedEntry);
          if (!pending.some(({ receiptHash }) => receiptHash === receipt.receiptHash)) pending.push(receipt);
        } catch { /* retention cleanup receipts are best effort; an unproven accumulator is preserved */ }
      }
      if (pending.length > MAX_PENDING_ACCUMULATOR_CLEANUPS) throw new RunEvaluationStoreConflictError(`Pending accumulator cleanup limit ${MAX_PENDING_ACCUMULATOR_CLEANUPS} reached`);
      const next = sealIndex(entries, pending);
      await this.publishImmutableEnvelope(envelope);
      await this.options.failpoint?.("before_envelope_index_commit");
      await this.verifyImmutableEnvelope(envelope);
      await this.writeIndex(next, index.indexHash);
      const obsolete = [...removedHashes];
      if (existing && existing.envelopeHash !== envelope.envelopeHash) obsolete.push(existing.envelopeHash);
      let prunedEnvelopeCount = 0;
      for (const hash of new Set(obsolete)) {
        if (next.entries.some(({ envelopeHash }) => envelopeHash === hash)) continue;
        try { if (await this.removeEnvelopeExpected(hash)) prunedEnvelopeCount += 1; } catch { /* telemetry cleanup is best effort */ }
      }
      return { index: next, prunedEnvelopeCount };
    });
  }

  async maintain(now: string): Promise<RunEvaluationMaintenanceResultV1> {
    const nowNanoseconds = parseRfc3339UtcNanosecondsV1(now);
    if (nowNanoseconds === null) throw new Error("maintenance now must be a real UTC RFC 3339 timestamp");
    await this.ensureDirectories();
    return this.withLock(async () => {
      const index = await this.readIndex();
      let prunedEnvelopeCount = 0;
      let deletedAccumulatorCount = 0;
      let cleanupErrorCount = 0;
      const sevenDaysNanoseconds = 7n * 24n * 60n * 60n * 1_000_000_000n;
      const eligible = (indexedAt: string): boolean => nowNanoseconds - parseRfc3339UtcNanosecondsV1(indexedAt)! >= sevenDaysNanoseconds;
      const terminal = index.entries.filter(({ cutoffKind, indexedAt }) => cutoffKind === "terminal" && eligible(indexedAt));
      for (const entry of terminal) {
        try {
          const accumulator = await this.readAccumulator(entry.runIdentityHash);
          if (!accumulator) continue;
          const envelope = await this.readEnvelope(entry.envelopeHash);
          if (!envelopeMatchesEntry(envelope, entry) || !accumulatorMatchesEnvelope(accumulator, envelope)) throw new RunEvaluationStoreCorruptError("Indexed terminal envelope does not exactly bind the current accumulator cleanup target");
          if (await this.unlinkAccumulatorExpected(entry.runIdentityHash, entry.expectedAccumulatorHash)) deletedAccumulatorCount += 1;
          else throw new RunEvaluationStoreCorruptError("Accumulator changed during exact terminal cleanup");
        } catch { cleanupErrorCount += 1; }
      }
      const retainedPending: PendingAccumulatorCleanupV1[] = [];
      for (const receipt of index.pendingAccumulatorCleanup) {
        if (!eligible(receipt.indexedAt)) { retainedPending.push(receipt); continue; }
        try {
          const accumulator = await this.readAccumulator(receipt.runIdentityHash);
          if (!accumulator) continue;
          if (!accumulatorMatchesReceipt(accumulator, receipt)) throw new RunEvaluationStoreCorruptError("Pending terminal receipt does not exactly bind the current accumulator cleanup target");
          if (await this.unlinkAccumulatorExpected(receipt.runIdentityHash, receipt.expectedAccumulatorHash)) deletedAccumulatorCount += 1;
          else throw new RunEvaluationStoreCorruptError("Accumulator changed during exact pending cleanup");
        } catch {
          cleanupErrorCount += 1;
          retainedPending.push(receipt);
        }
      }
      if (retainedPending.length !== index.pendingAccumulatorCleanup.length) await this.writeIndex(sealIndex(index.entries, retainedPending), index.indexHash);
      const indexedHashes = new Set(index.entries.map(({ envelopeHash }) => envelopeHash));
      await this.assertStoreDirectories();
      for (const name of await readdir(this.envelopesDirectory)) {
        if (!/^[0-9a-f]{64}\.json$/.test(name)) continue;
        const hash = `sha256:${name.slice(0, 64)}`;
        if (indexedHashes.has(hash)) continue;
        try {
          const envelopeValue = await this.readEnvelope(hash);
          if (envelopeValue.envelopeHash !== hash) continue;
          await this.unlinkRegularFileExpected(this.envelopePath(hash), hash, async () => (await this.readEnvelope(hash)).envelopeHash);
          prunedEnvelopeCount += 1;
        } catch { cleanupErrorCount += 1; }
      }
      if (prunedEnvelopeCount > 0) await this.fsyncBoundDirectory(this.envelopesDirectory, this.requireBindings().envelopes);
      return { prunedEnvelopeCount, deletedAccumulatorCount, cleanupErrorCount };
    });
  }

  private async ensureDirectories(): Promise<void> {
    if (!this.bindings) await this.initialize();
    else await this.assertStoreDirectories();
  }
  private accumulatorPath(runIdentityHash: string): string { return join(this.accumulatorsDirectory, `${runIdentityHash.slice("sha256:".length)}.json`); }
  private envelopePath(hash: string): string { return join(this.envelopesDirectory, `${hash.slice("sha256:".length)}.json`); }

  private async prepareAndBindDirectories(): Promise<void> {
    if (this.bindings) { await this.assertStoreDirectories(); return; }
    const project = await inspectExactDirectory(this.projectDirectory, "project root");
    if (await realpath(this.projectDirectory) !== this.projectDirectory) throw new RunEvaluationStoreCorruptError("Project root must be an exact canonical path, not a symlink alias");
    const ai = await ensureExactChildDirectory(this.projectDirectory, project, this.aiDirectory, ".ai directory");
    const root = await ensureExactChildDirectory(this.aiDirectory, ai, this.rootDirectory, "evaluation root");
    const accumulators = await ensureExactChildDirectory(this.rootDirectory, root, this.accumulatorsDirectory, "accumulator directory");
    const envelopes = await ensureExactChildDirectory(this.rootDirectory, root, this.envelopesDirectory, "envelope directory");
    this.bindings = {
      project, ai, root, accumulators, envelopes,
      rootIdentityHash: canonicalHash({ canonicalProjectPathHash: canonicalHash(this.projectDirectory), project: identityHash(project), root: identityHash(root) }),
    };
    await this.assertStoreDirectories();
  }

  private requireBindings(): StorePathBindings {
    if (!this.bindings) throw new RunEvaluationStoreCorruptError("Evaluation store directory identity is not initialized");
    return this.bindings;
  }

  private async assertStoreDirectories(): Promise<void> {
    const binding = this.requireBindings();
    await assertExactDirectory(this.projectDirectory, binding.project, "project root");
    await assertExactDirectory(this.aiDirectory, binding.ai, ".ai directory");
    await assertExactDirectory(this.rootDirectory, binding.root, "evaluation root");
    await assertExactDirectory(this.accumulatorsDirectory, binding.accumulators, "accumulator directory");
    await assertExactDirectory(this.envelopesDirectory, binding.envelopes, "envelope directory");
  }

  private async safeFileExists(path: string, label: string): Promise<boolean> {
    await this.assertStoreDirectories();
    try {
      const stats = await lstat(path, { bigint: true });
      if (stats.isSymbolicLink() || !stats.isFile()) throw new RunEvaluationStoreCorruptError(`${label} must be a regular non-symlink file`);
      return true;
    } catch (error: any) { if (error?.code === "ENOENT") return false; throw error; }
  }

  private async readRegularFileNoFollow(path: string, label: string): Promise<string> {
    await this.assertStoreDirectories();
    const before = await inspectRegularFile(path, label);
    const handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
    try {
      const opened = identityFromStats(await handle.stat({ bigint: true }));
      if (!sameIdentity(before, opened)) throw new RunEvaluationStoreCorruptError(`${label} changed before no-follow open`);
      const text = await handle.readFile("utf8");
      const after = await inspectRegularFile(path, label);
      if (!sameIdentity(opened, after)) throw new RunEvaluationStoreCorruptError(`${label} changed while being read`);
      await this.assertStoreDirectories();
      return text;
    } finally { await handle.close(); }
  }

  private async publishImmutableEnvelope(envelope: RunEvaluationEnvelopeV1): Promise<void> {
    const path = this.envelopePath(envelope.envelopeHash);
    const text = canonicalStringify(envelope);
    const temporary = `${path}.${process.pid}.${randomUUID()}.tmp`;
    let handle;
    try {
      await this.assertStoreDirectories();
      handle = await open(temporary, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
      await handle.writeFile(text, "utf8");
      await handle.sync();
      await this.options.failpoint?.("after_envelope_temp_sync");
      await handle.close(); handle = undefined;
      await this.assertStoreDirectories();
      await inspectRegularFile(temporary, "temporary evaluation envelope");
      try { await link(temporary, path); }
      catch (error: any) {
        if (error?.code !== "EEXIST") throw error;
        if (await this.readRegularFileNoFollow(path, "evaluation envelope") !== text) throw new RunEvaluationStoreCorruptError(`Envelope hash collision at ${envelope.envelopeHash}`);
      }
      await this.assertStoreDirectories();
      await this.fsyncBoundDirectory(this.envelopesDirectory, this.requireBindings().envelopes);
      await this.options.failpoint?.("after_envelope_publish");
    } finally {
      await handle?.close().catch(() => undefined);
      await this.removeTemporaryRegularFile(temporary, this.envelopesDirectory, this.requireBindings().envelopes);
    }
  }

  private async verifyImmutableEnvelope(envelope: RunEvaluationEnvelopeV1): Promise<void> {
    const expectedBytes = canonicalStringify(envelope);
    const actualBytes = await this.readRegularFileNoFollow(this.envelopePath(envelope.envelopeHash), "evaluation envelope");
    if (actualBytes !== expectedBytes || runEvaluationEnvelopeHashV1(envelope) !== envelope.envelopeHash) {
      throw new RunEvaluationStoreCorruptError(`Published envelope does not match exact canonical bytes at ${envelope.envelopeHash}`);
    }
  }

  private async writeIndex(index: RunEvaluationIndexV1, expectedIndexHash: string | null): Promise<void> {
    await this.atomicReplace(this.indexPath, canonicalStringify(index), "index", expectedIndexHash);
  }

  private async removeEnvelopeExpected(hash: string): Promise<boolean> {
    return this.unlinkRegularFileExpected(this.envelopePath(hash), hash, async () => {
      const envelope = await this.readEnvelope(hash);
      return runEvaluationEnvelopeHashV1(envelope);
    });
  }

  private async unlinkAccumulatorExpected(runIdentityHash: string, expectedHash: string): Promise<boolean> {
    return this.unlinkRegularFileExpected(this.accumulatorPath(runIdentityHash), expectedHash, async () => (await this.readAccumulator(runIdentityHash))?.accumulatorHash ?? null);
  }

  private async unlinkRegularFileExpected(path: string, expectedHash: string, readHash: () => Promise<string | null>): Promise<boolean> {
    await this.assertStoreDirectories();
    if (await readHash() !== expectedHash) return false;
    const identity = await inspectRegularFile(path, "cleanup target");
    await this.assertStoreDirectories();
    if (await readHash() !== expectedHash) return false;
    const unchanged = await inspectRegularFile(path, "cleanup target");
    if (!sameIdentity(identity, unchanged)) return false;
    await unlink(path);
    await this.assertStoreDirectories();
    const parent = dirname(path);
    const bindings = this.requireBindings();
    await this.fsyncBoundDirectory(parent, parent === this.accumulatorsDirectory ? bindings.accumulators : bindings.envelopes);
    return true;
  }

  private async atomicReplace(path: string, text: string, kind: "accumulator" | "index", expectedCurrentHash: string | null): Promise<void> {
    const temporary = `${path}.${process.pid}.${randomUUID()}.tmp`;
    let handle;
    try {
      await this.assertStoreDirectories();
      handle = await open(temporary, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
      await handle.writeFile(text, "utf8");
      await handle.sync();
      await this.options.failpoint?.(kind === "accumulator" ? "after_accumulator_temp_sync" : "after_index_temp_sync");
      await handle.close(); handle = undefined;
      await this.assertStoreDirectories();
      await inspectRegularFile(temporary, `temporary ${kind}`);
      const currentHash = await this.currentStoredHash(path, kind);
      if (currentHash !== expectedCurrentHash) throw new RunEvaluationStoreConflictError(`${kind} changed before atomic publication`);
      await this.assertStoreDirectories();
      await rename(temporary, path);
      await this.options.failpoint?.(kind === "accumulator" ? "after_accumulator_rename" : "after_index_rename");
      await this.assertStoreDirectories();
      await inspectRegularFile(path, kind);
      const parent = dirname(path);
      const bindings = this.requireBindings();
      await this.fsyncBoundDirectory(parent, parent === this.accumulatorsDirectory ? bindings.accumulators : bindings.root);
    } finally {
      await handle?.close().catch(() => undefined);
      const bindings = this.requireBindings();
      const parent = dirname(path);
      await this.removeTemporaryRegularFile(temporary, parent, parent === this.accumulatorsDirectory ? bindings.accumulators : bindings.root);
    }
  }

  private async removeTemporaryRegularFile(path: string, parent: string, expectedParent: PathIdentity): Promise<void> {
    try {
      await assertExactDirectory(parent, expectedParent, "temporary file parent");
      await inspectRegularFile(path, "temporary evaluation file");
      await unlink(path);
      await assertExactDirectory(parent, expectedParent, "temporary file parent");
    } catch (error: any) { if (error?.code !== "ENOENT") throw error; }
  }

  private async currentStoredHash(path: string, kind: "accumulator" | "index"): Promise<string | null> {
    try {
      const raw = await this.readRegularFileNoFollow(path, kind);
      const value = parseStrictJson(raw) as Record<string, unknown>;
      const hash = kind === "accumulator" ? value.accumulatorHash : value.indexHash;
      if (typeof hash !== "string" || !HASH_PATTERN.test(hash)) throw new RunEvaluationStoreCorruptError(`${kind} has no valid content hash`);
      return hash;
    } catch (error: any) {
      if (error?.causeValue?.code === "ENOENT" || error?.code === "ENOENT") return null;
      throw error;
    }
  }

  private async fsyncBoundDirectory(path: string, expected: PathIdentity): Promise<void> {
    await assertExactDirectory(path, expected, "fsync directory");
    const handle = await open(path, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
    try {
      const opened = identityFromStats(await handle.stat({ bigint: true }));
      if (!sameIdentity(opened, expected)) throw new RunEvaluationStoreCorruptError("Directory identity changed before fsync");
      await handle.sync();
    } finally { await handle.close(); }
    await assertExactDirectory(path, expected, "fsync directory");
  }

  private async withLock<T>(operation: () => Promise<T>): Promise<T> {
    await this.assertStoreDirectories();
    await this.recoverStaleClaims();
    let owned: OwnedLock | null = null;
    for (let attempt = 0; attempt < 3 && !owned; attempt += 1) {
      owned = await this.tryAcquireLock();
      if (owned) break;
      const existing = await this.inspectOwnedLock(this.lockDirectory).catch((error: any) => {
        if (error?.code === "ENOENT") return null;
        throw error;
      });
      if (!existing) continue;
      const status = await processIdentityStatus(existing.owner.pid, existing.owner.processStartIdentity);
      if (status === "live" || status === "ambiguous") throw new RunEvaluationStoreBusyError();
      await this.retireExactLock(this.lockDirectory, existing);
    }
    if (!owned) throw new RunEvaluationStoreBusyError();
    let operationError: unknown;
    try { return await operation(); }
    catch (error) { operationError = error; throw error; }
    finally {
      try { await this.releaseExactLock(owned); }
      catch (releaseError) { if (operationError === undefined) throw releaseError; }
    }
  }

  private async tryAcquireLock(): Promise<OwnedLock | null> {
    const bindings = this.requireBindings();
    const processStartIdentity = await requireOwnProcessStartIdentity();
    const token = randomUUID();
    const claimPath = lockClaimPath(this.rootDirectory, process.pid, processStartIdentity, token);
    await this.assertStoreDirectories();
    await mkdir(claimPath, { mode: 0o700 });
    let published = false;
    try {
      const identity = await inspectExactDirectory(claimPath, "lock claim");
      const core = {
        schemaVersion: 1 as const,
        kind: "run_evaluation_store_lock_owner" as const,
        pid: process.pid,
        processStartIdentity,
        ownerTokenHash: canonicalHash(token),
        rootIdentityHash: bindings.rootIdentityHash,
        lockDirectoryIdentityHash: identityHash(identity),
      };
      const owner: LockOwnerV1 = { ...core, metadataHash: canonicalHash(core) };
      const ownerPath = join(claimPath, "owner.json");
      const handle = await open(ownerPath, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
      try { await handle.writeFile(canonicalStringify(owner), "utf8"); await handle.sync(); } finally { await handle.close(); }
      await fsyncExactDirectory(claimPath, identity);
      const inspected = await this.inspectOwnedLock(claimPath);
      if (inspected.owner.ownerTokenHash !== owner.ownerTokenHash) throw new RunEvaluationStoreCorruptError("Lock claim owner changed before publication");
      if (await requireOwnProcessStartIdentity() !== processStartIdentity) throw new RunEvaluationStoreCorruptError("Executing process identity changed during lock publication");
      await this.assertStoreDirectories();
      try { await rename(claimPath, this.lockDirectory); }
      catch (error: any) { if (["EEXIST", "ENOTEMPTY"].includes(error?.code)) return null; throw error; }
      published = true;
      await this.fsyncBoundDirectory(this.rootDirectory, bindings.root);
      const acquired = await this.inspectOwnedLock(this.lockDirectory);
      if (!sameIdentity(acquired.identity, identity) || acquired.owner.ownerTokenHash !== owner.ownerTokenHash) throw new RunEvaluationStoreCorruptError("Published lock identity does not match the process-owned claim");
      return acquired;
    } finally {
      if (!published) await this.removeUnpublishedClaim(claimPath).catch(() => undefined);
    }
  }

  private async inspectOwnedLock(path: string): Promise<OwnedLock> {
    await this.assertStoreDirectories();
    const identity = await inspectExactDirectory(path, "evaluation lock");
    const raw = await readRegularFileNoFollowStandalone(join(path, "owner.json"), "evaluation lock owner");
    const owner = parseLockOwner(raw);
    if (owner.rootIdentityHash !== this.requireBindings().rootIdentityHash) throw new RunEvaluationStoreCorruptError("Evaluation lock belongs to a different canonical repository/root identity");
    if (owner.lockDirectoryIdentityHash !== identityHash(identity)) throw new RunEvaluationStoreCorruptError("Evaluation lock owner does not bind the exact lock directory identity");
    const after = await inspectExactDirectory(path, "evaluation lock");
    if (!sameIdentity(identity, after)) throw new RunEvaluationStoreCorruptError("Evaluation lock changed during inspection");
    return { identity, owner };
  }

  private async recoverStaleClaims(): Promise<void> {
    await this.assertStoreDirectories();
    const names = (await readdir(this.rootDirectory)).filter((name) => name.startsWith(LOCK_CLAIM_PREFIX));
    if (names.length > LOCK_CLAIM_LIMIT) throw new RunEvaluationStoreBusyError();
    for (const name of names.sort()) {
      const namedIdentity = parseLockClaimName(name);
      if (!namedIdentity) throw new RunEvaluationStoreCorruptError("Ambiguous evaluation lock claim residue");
      const path = join(this.rootDirectory, name);
      let claim: OwnedLock | null = null;
      try { claim = await this.inspectOwnedLock(path); }
      catch (error: any) {
        if (error?.code !== "ENOENT") throw error;
      }
      if (claim) {
        if (claim.owner.pid !== namedIdentity.pid || claim.owner.processStartIdentity !== namedIdentity.processStartIdentity) throw new RunEvaluationStoreCorruptError("Lock claim name and owner process identity disagree");
        const status = await processIdentityStatus(claim.owner.pid, claim.owner.processStartIdentity);
        if (status === "dead" || status === "reused") await this.retireExactLock(path, claim);
        continue;
      }
      const status = await processIdentityStatus(namedIdentity.pid, namedIdentity.processStartIdentity);
      if (status === "dead" || status === "reused") await this.retireOwnerlessClaim(path);
    }
  }

  private async retireExactLock(path: string, lock: OwnedLock): Promise<void> {
    await this.assertStoreDirectories();
    const current = await this.inspectOwnedLock(path);
    if (!sameIdentity(current.identity, lock.identity) || current.owner.ownerTokenHash !== lock.owner.ownerTokenHash) throw new RunEvaluationStoreBusyError();
    const quarantine = lockClaimPath(this.rootDirectory, lock.owner.pid, lock.owner.processStartIdentity, randomUUID());
    await rename(path, quarantine);
    const moved = await this.inspectOwnedLock(quarantine);
    if (!sameIdentity(moved.identity, lock.identity) || moved.owner.ownerTokenHash !== lock.owner.ownerTokenHash) throw new RunEvaluationStoreCorruptError("Stale lock changed during exact recovery");
    await unlink(join(quarantine, "owner.json"));
    await rmdir(quarantine);
    await this.fsyncBoundDirectory(this.rootDirectory, this.requireBindings().root);
  }

  private async releaseExactLock(lock: OwnedLock): Promise<void> {
    if (await requireOwnProcessStartIdentity() !== lock.owner.processStartIdentity || process.pid !== lock.owner.pid) throw new RunEvaluationStoreCorruptError("Only the exact acquiring process may release the evaluation lock");
    const current = await this.inspectOwnedLock(this.lockDirectory);
    if (!sameIdentity(current.identity, lock.identity) || current.owner.ownerTokenHash !== lock.owner.ownerTokenHash) throw new RunEvaluationStoreCorruptError("Evaluation lock changed before release");
    await this.retireExactLock(this.lockDirectory, lock);
  }

  private async retireOwnerlessClaim(path: string): Promise<void> {
    const identity = await inspectExactDirectory(path, "ownerless stale lock claim");
    if ((await readdir(path)).length !== 0) throw new RunEvaluationStoreCorruptError("Ownerless stale lock claim contains ambiguous entries");
    await assertExactDirectory(path, identity, "ownerless stale lock claim");
    await rmdir(path);
    await this.fsyncBoundDirectory(this.rootDirectory, this.requireBindings().root);
  }

  private async removeUnpublishedClaim(path: string): Promise<void> {
    try {
      const identity = await inspectExactDirectory(path, "unpublished lock claim");
      const names = await readdir(path);
      if (names.some((name) => name !== "owner.json")) throw new RunEvaluationStoreCorruptError("Unpublished lock claim contains ambiguous entries");
      if (names.includes("owner.json")) await unlink(join(path, "owner.json"));
      await assertExactDirectory(path, identity, "unpublished lock claim");
      await rmdir(path);
    } catch (error: any) { if (error?.code !== "ENOENT") throw error; }
  }
}

interface ObserverAccountingV1 { dropped: number; failures: number; }
interface QueuedCommittedSnapshotIdentityV1 extends CommittedSnapshotIdentityV1 { accountingKey: string; }

export class RunEvaluationObserverV1 {
  private readonly store: RunEvaluationStoreV1;
  private readonly loadObservation: (identity: CommittedSnapshotIdentityV1) => Promise<AccumulatorObservationV1 | null>;
  private readonly boundIdentity: Readonly<RunEvaluationObserverIdentityV1> | null;
  private readonly accounting = new Map<string, ObserverAccountingV1>();
  private running: Promise<void> | null = null;
  private runningIdentity: QueuedCommittedSnapshotIdentityV1 | null = null;
  private pending: QueuedCommittedSnapshotIdentityV1 | null = null;

  constructor(store: RunEvaluationStoreV1, loadObservation: (identity: CommittedSnapshotIdentityV1) => Promise<AccumulatorObservationV1 | null>);
  constructor(store: RunEvaluationStoreV1, identity: RunEvaluationObserverIdentityV1, loadObservation: (identity: CommittedSnapshotIdentityV1) => Promise<AccumulatorObservationV1 | null>);
  constructor(
    store: RunEvaluationStoreV1,
    identityOrLoader: RunEvaluationObserverIdentityV1 | ((identity: CommittedSnapshotIdentityV1) => Promise<AccumulatorObservationV1 | null>),
    maybeLoader?: (identity: CommittedSnapshotIdentityV1) => Promise<AccumulatorObservationV1 | null>,
  ) {
    this.store = store;
    if (typeof identityOrLoader === "function") {
      this.boundIdentity = null;
      this.loadObservation = identityOrLoader;
    } else {
      this.boundIdentity = Object.freeze(validateObserverIdentity(identityOrLoader));
      if (!maybeLoader) throw new Error("A bound evaluation observer requires an observation loader");
      this.loadObservation = maybeLoader;
    }
  }

  offerCommittedSnapshot(identity: CommittedSnapshotIdentityV1): void {
    const offered = this.validateOffer(identity);
    this.requireAccounting(offered.accountingKey);
    if (this.running) {
      if (this.pending) this.requireAccounting(this.pending.accountingKey).dropped += 1;
      this.pending = offered;
      return;
    }
    this.start(offered);
  }

  async flush(): Promise<void> { while (this.running) await this.running; }

  private validateOffer(identity: CommittedSnapshotIdentityV1): QueuedCommittedSnapshotIdentityV1 {
    if (!identity || typeof identity !== "object" || Array.isArray(identity)) throw new Error("Committed snapshot offer must be a plain identity object");
    const offerSchema = this.boundIdentity ? BoundCommittedSnapshotIdentityV1Schema : MinimalCommittedSnapshotIdentityV1Schema;
    const issues = schemaIssues(offerSchema, identity);
    if (issues.length) throw new Error(`Invalid committed snapshot offer: ${issues.map(({ path, message }) => `${path || "/"} ${message}`).join("; ")}`);
    if (this.boundIdentity) {
      const offeredIdentity = validateObserverIdentity(identity);
      if (canonicalStringify(offeredIdentity) !== canonicalStringify(this.boundIdentity)) throw new Error("Committed snapshot does not match the observer's immutable run/evaluation identity");
    }
    const accountingKey = this.boundIdentity ? canonicalHash(this.boundIdentity) : identity.runIdentityHash;
    const fresh: QueuedCommittedSnapshotIdentityV1 = this.boundIdentity ? {
      runIdentityHash: identity.runIdentityHash,
      revision: identity.revision,
      snapshotHash: identity.snapshotHash,
      projectIdentityHash: identity.projectIdentityHash,
      runNonceHash: identity.runNonceHash,
      planHash: identity.planHash,
      evaluationProfileHash: identity.evaluationProfileHash,
      clockPolicyHash: identity.clockPolicyHash,
      creditContextHash: identity.creditContextHash,
      accountingKey,
    } : {
      runIdentityHash: identity.runIdentityHash,
      revision: identity.revision,
      snapshotHash: identity.snapshotHash,
      accountingKey,
    };
    return Object.freeze(fresh);
  }

  private requireAccounting(key: string): ObserverAccountingV1 {
    const current = this.accounting.get(key);
    if (current) return current;
    if (this.accounting.size >= OBSERVER_RUN_ACCOUNTING_LIMIT) throw new Error(`Evaluation observer run accounting limit ${OBSERVER_RUN_ACCOUNTING_LIMIT} reached`);
    const created = { dropped: 0, failures: 0 };
    this.accounting.set(key, created);
    return created;
  }

  private releaseEmptyAccounting(key: string): void {
    const current = this.accounting.get(key);
    if (!current || current.dropped !== 0 || current.failures !== 0) return;
    if (this.runningIdentity?.accountingKey === key || this.pending?.accountingKey === key) return;
    this.accounting.delete(key);
  }

  private start(identity: QueuedCommittedSnapshotIdentityV1): void {
    this.runningIdentity = identity;
    const deferred = new Promise<void>((resolve) => queueMicrotask(resolve));
    this.running = deferred.then(() => this.process(identity)).catch(() => {
      this.requireAccounting(identity.accountingKey).failures += 1;
    }).finally(() => {
      this.running = null;
      this.runningIdentity = null;
      const pending = this.pending;
      this.pending = null;
      if (pending) this.start(pending);
      this.releaseEmptyAccounting(identity.accountingKey);
    });
  }

  private async process(identity: QueuedCommittedSnapshotIdentityV1): Promise<void> {
    if (this.boundIdentity) {
      const accumulator = await this.store.readAccumulator(identity.runIdentityHash);
      if (!accumulator || canonicalStringify(observerIdentityFromAccumulator(accumulator)) !== canonicalStringify(this.boundIdentity)) {
        throw new Error("Observer store head does not match its immutable run/evaluation identity");
      }
    }
    const { accountingKey: _accountingKey, ...committedIdentity } = identity;
    const observation = await this.loadObservation(committedIdentity);
    if (!observation || observation.revision !== identity.revision || observation.snapshotHash !== identity.snapshotHash) throw new Error("Observer did not produce the exact committed snapshot observation");
    assertAccumulatorObservationV1(observation);
    if (this.boundIdentity && canonicalHash(observation.creditContext) !== this.boundIdentity.creditContextHash) {
      throw new Error("Observer loaded an observation outside its immutable v1 credit context");
    }
    const accounting = this.requireAccounting(identity.accountingKey);
    const pendingDropped = accounting.dropped;
    const pendingFailures = accounting.failures;
    const result = await this.store.foldObservation(identity.runIdentityHash, {
      ...observation,
      droppedBefore: observation.droppedBefore + pendingDropped,
      observerFailuresBefore: observation.observerFailuresBefore + pendingFailures,
    });
    if (result.folded) {
      accounting.dropped -= pendingDropped;
      accounting.failures -= pendingFailures;
    }
  }
}

function validateObserverIdentity(value: Partial<RunEvaluationObserverIdentityV1>): RunEvaluationObserverIdentityV1 {
  const identity = {
    projectIdentityHash: value.projectIdentityHash,
    runIdentityHash: value.runIdentityHash,
    runNonceHash: value.runNonceHash,
    planHash: value.planHash,
    evaluationProfileHash: value.evaluationProfileHash,
    clockPolicyHash: value.clockPolicyHash,
    creditContextHash: value.creditContextHash,
  };
  const issues = schemaIssues(RunEvaluationObserverIdentityV1Schema, identity);
  if (issues.length) throw new Error(`Invalid evaluation observer identity: ${issues.map(({ path, message }) => `${path || "/"} ${message}`).join("; ")}`);
  return identity as RunEvaluationObserverIdentityV1;
}

function observerIdentityFromAccumulator(accumulator: RunObservationAccumulatorV1): RunEvaluationObserverIdentityV1 {
  return { ...accumulator.identity, creditContextHash: accumulator.creditContextHash };
}

function accumulatorImmutableIdentity(accumulator: RunObservationAccumulatorV1): unknown {
  return {
    schemaVersion: accumulator.schemaVersion,
    kind: accumulator.kind,
    canonicalization: accumulator.canonicalization,
    evaluationProfileHash: accumulator.identity.evaluationProfileHash,
    projectIdentityHash: accumulator.identity.projectIdentityHash,
    runIdentityHash: accumulator.identity.runIdentityHash,
    runNonceHash: accumulator.identity.runNonceHash,
    planHash: accumulator.identity.planHash,
    clockPolicyHash: accumulator.identity.clockPolicyHash,
    creditContext: accumulator.creditContext,
    creditContextHash: accumulator.creditContextHash,
  };
}

function entryFromEnvelope(envelope: RunEvaluationEnvelopeV1, indexedAt: string): EvaluationIndexEntryV1 {
  return {
    cutoffIdentityHash: envelope.cutoff.cutoffIdentityHash,
    envelopeHash: envelope.envelopeHash,
    projectIdentityHash: envelope.identity.projectIdentityHash,
    runIdentityHash: envelope.identity.runIdentityHash,
    runNonceHash: envelope.identity.runNonceHash,
    planHash: envelope.identity.planHash,
    evaluationProfileHash: envelope.evaluationProfile.profileHash,
    sourceRevision: envelope.source.revision,
    sourceSnapshotHash: envelope.source.snapshotHash,
    expectedAccumulatorHash: envelope.source.accumulatorHash,
    creditContextHash: envelope.creditContextHash,
    cutoffAt: envelope.cutoff.cutoffAt,
    cutoffKind: envelope.cutoff.kind,
    indexedAt,
  };
}
function sealCleanupReceipt(entry: EvaluationIndexEntryV1): PendingAccumulatorCleanupV1 {
  const core = {
    terminalEnvelopeHash: entry.envelopeHash,
    cutoffIdentityHash: entry.cutoffIdentityHash,
    cutoffKind: "terminal" as const,
    cutoffAt: entry.cutoffAt,
    projectIdentityHash: entry.projectIdentityHash,
    runIdentityHash: entry.runIdentityHash,
    runNonceHash: entry.runNonceHash,
    planHash: entry.planHash,
    evaluationProfileHash: entry.evaluationProfileHash,
    sourceRevision: entry.sourceRevision,
    sourceSnapshotHash: entry.sourceSnapshotHash,
    expectedAccumulatorHash: entry.expectedAccumulatorHash,
    creditContextHash: entry.creditContextHash,
    indexedAt: entry.indexedAt,
  };
  return { ...core, receiptHash: canonicalHash(core) };
}
function sealIndex(entries: RunEvaluationIndexV1["entries"], pendingAccumulatorCleanup: RunEvaluationIndexV1["pendingAccumulatorCleanup"]): RunEvaluationIndexV1 {
  const core = {
    schemaVersion: 1 as const,
    kind: "run_evaluation_index" as const,
    canonicalization: "jcs-v1" as const,
    entries: [...entries].sort(compareIndexIdentity),
    pendingAccumulatorCleanup: [...pendingAccumulatorCleanup].sort((a, b) => a.receiptHash.localeCompare(b.receiptHash)),
  };
  return { ...core, indexHash: canonicalHash(core) };
}
function validateIndex(value: unknown): { path: string; message: string }[] {
  const issues = schemaIssues(RunEvaluationIndexV1Schema, value);
  if (!issues.length) {
    const index = value as RunEvaluationIndexV1;
    validateEvaluationIndexTimestamps(index, issues);
    if (index.indexHash !== hashWithoutField(index as unknown as Record<string, unknown>, "indexHash")) issues.push({ path: "/indexHash", message: "must match canonical content excluding indexHash" });
    const sorted = [...index.entries].sort(compareIndexIdentity);
    if (canonicalStringify(sorted) !== canonicalStringify(index.entries)) issues.push({ path: "/entries", message: "must use deterministic identity order" });
    if (new Set(index.entries.map(({ cutoffIdentityHash }) => cutoffIdentityHash)).size !== index.entries.length) issues.push({ path: "/entries", message: "cutoff identities must be unique" });
    const projectCounts = new Map<string, number>();
    for (const entry of index.entries) projectCounts.set(entry.projectIdentityHash, (projectCounts.get(entry.projectIdentityHash) ?? 0) + 1);
    if ([...projectCounts.values()].some((count) => count > 50)) issues.push({ path: "/entries", message: "cannot retain more than 50 current envelope heads per project" });
    const pendingSorted = [...index.pendingAccumulatorCleanup].sort((a, b) => a.receiptHash.localeCompare(b.receiptHash));
    if (canonicalStringify(pendingSorted) !== canonicalStringify(index.pendingAccumulatorCleanup)) issues.push({ path: "/pendingAccumulatorCleanup", message: "must use deterministic receipt-hash order" });
    if (new Set(index.pendingAccumulatorCleanup.map(({ receiptHash }) => receiptHash)).size !== index.pendingAccumulatorCleanup.length) issues.push({ path: "/pendingAccumulatorCleanup", message: "receipt hashes must be unique" });
    index.pendingAccumulatorCleanup.forEach((receipt, itemIndex) => {
      const { receiptHash, ...core } = receipt;
      if (receiptHash !== canonicalHash(core)) issues.push({ path: `/pendingAccumulatorCleanup/${itemIndex}/receiptHash`, message: "must bind the exact terminal accumulator cleanup identity" });
    });
  }
  return issues;
}
function compareIndexIdentity(a: EvaluationIndexEntryV1, b: EvaluationIndexEntryV1): number {
  return a.projectIdentityHash.localeCompare(b.projectIdentityHash) || compareRetentionIdentity(a, b);
}
function compareRetentionIdentity(a: EvaluationIndexEntryV1, b: EvaluationIndexEntryV1): number {
  const timeOrder = compareNanoseconds(parseRfc3339UtcNanosecondsV1(a.cutoffAt)!, parseRfc3339UtcNanosecondsV1(b.cutoffAt)!);
  return timeOrder || a.sourceRevision - b.sourceRevision || a.runIdentityHash.localeCompare(b.runIdentityHash) || a.runNonceHash.localeCompare(b.runNonceHash) || a.cutoffIdentityHash.localeCompare(b.cutoffIdentityHash) || a.envelopeHash.localeCompare(b.envelopeHash);
}
function compareNanoseconds(a: bigint, b: bigint): number { return a < b ? -1 : a > b ? 1 : 0; }
function validateEvaluationIndexTimestamps(value: RunEvaluationIndexV1, issues: { path: string; message: string }[]): void {
  value.entries.forEach((entry, index) => {
    if (parseRfc3339UtcNanosecondsV1(entry.cutoffAt) === null) issues.push({ path: `/entries/${index}/cutoffAt`, message: "must be a real UTC RFC 3339 civil timestamp" });
    if (parseRfc3339UtcNanosecondsV1(entry.indexedAt) === null) issues.push({ path: `/entries/${index}/indexedAt`, message: "must be a real UTC RFC 3339 civil timestamp" });
  });
  value.pendingAccumulatorCleanup.forEach((entry, index) => {
    if (parseRfc3339UtcNanosecondsV1(entry.cutoffAt) === null) issues.push({ path: `/pendingAccumulatorCleanup/${index}/cutoffAt`, message: "must be a real UTC RFC 3339 civil timestamp" });
    if (parseRfc3339UtcNanosecondsV1(entry.indexedAt) === null) issues.push({ path: `/pendingAccumulatorCleanup/${index}/indexedAt`, message: "must be a real UTC RFC 3339 civil timestamp" });
  });
}
function envelopeMatchesEntry(envelope: RunEvaluationEnvelopeV1, entry: EvaluationIndexEntryV1): boolean {
  return envelope.envelopeHash === entry.envelopeHash
    && envelope.cutoff.kind === "terminal"
    && envelope.cutoff.cutoffIdentityHash === entry.cutoffIdentityHash
    && envelope.cutoff.cutoffAt === entry.cutoffAt
    && envelope.identity.projectIdentityHash === entry.projectIdentityHash
    && envelope.identity.runIdentityHash === entry.runIdentityHash
    && envelope.identity.runNonceHash === entry.runNonceHash
    && envelope.identity.planHash === entry.planHash
    && envelope.evaluationProfile.profileHash === entry.evaluationProfileHash
    && envelope.source.revision === entry.sourceRevision
    && envelope.source.snapshotHash === entry.sourceSnapshotHash
    && envelope.source.accumulatorHash === entry.expectedAccumulatorHash
    && envelope.creditContextHash === entry.creditContextHash;
}
function accumulatorMatchesEnvelope(accumulator: RunObservationAccumulatorV1, envelope: RunEvaluationEnvelopeV1): boolean {
  return accumulator.accumulatorHash === envelope.source.accumulatorHash
    && accumulator.identity.projectIdentityHash === envelope.identity.projectIdentityHash
    && accumulator.identity.runIdentityHash === envelope.identity.runIdentityHash
    && accumulator.identity.runNonceHash === envelope.identity.runNonceHash
    && accumulator.identity.planHash === envelope.identity.planHash
    && accumulator.identity.evaluationProfileHash === envelope.evaluationProfile.profileHash
    && accumulator.creditContextHash === envelope.creditContextHash
    && accumulator.source.revision === envelope.source.revision
    && accumulator.source.snapshotHash === envelope.source.snapshotHash;
}
function accumulatorMatchesReceipt(accumulator: RunObservationAccumulatorV1, receipt: PendingAccumulatorCleanupV1): boolean {
  return accumulator.accumulatorHash === receipt.expectedAccumulatorHash
    && accumulator.identity.projectIdentityHash === receipt.projectIdentityHash
    && accumulator.identity.runIdentityHash === receipt.runIdentityHash
    && accumulator.identity.runNonceHash === receipt.runNonceHash
    && accumulator.identity.planHash === receipt.planHash
    && accumulator.identity.evaluationProfileHash === receipt.evaluationProfileHash
    && accumulator.creditContextHash === receipt.creditContextHash
    && accumulator.source.revision === receipt.sourceRevision
    && accumulator.source.snapshotHash === receipt.sourceSnapshotHash;
}

function identityFromStats(stats: { dev: bigint; ino: bigint }): PathIdentity { return { device: stats.dev.toString(), inode: stats.ino.toString() }; }
function sameIdentity(a: PathIdentity, b: PathIdentity): boolean { return a.device === b.device && a.inode === b.inode; }
function identityHash(identity: PathIdentity): string { return canonicalHash({ device: identity.device, inode: identity.inode }); }
async function inspectExactDirectory(path: string, label: string): Promise<PathIdentity> {
  const stats = await lstat(path, { bigint: true });
  if (stats.isSymbolicLink() || !stats.isDirectory()) throw new RunEvaluationStoreCorruptError(`${label} must be a non-symlink directory`);
  if (await realpath(path) !== path) throw new RunEvaluationStoreCorruptError(`${label} must retain its exact canonical path`);
  return identityFromStats(stats);
}
async function assertExactDirectory(path: string, expected: PathIdentity, label: string): Promise<void> {
  const actual = await inspectExactDirectory(path, label);
  if (!sameIdentity(actual, expected)) throw new RunEvaluationStoreCorruptError(`${label} device/inode identity changed`);
}
async function ensureExactChildDirectory(parentPath: string, parentIdentity: PathIdentity, childPath: string, label: string): Promise<PathIdentity> {
  await assertExactDirectory(parentPath, parentIdentity, `${label} parent`);
  try { await mkdir(childPath, { mode: 0o700 }); }
  catch (error: any) { if (error?.code !== "EEXIST") throw error; }
  await assertExactDirectory(parentPath, parentIdentity, `${label} parent`);
  return inspectExactDirectory(childPath, label);
}
async function inspectRegularFile(path: string, label: string): Promise<PathIdentity> {
  const stats = await lstat(path, { bigint: true });
  if (stats.isSymbolicLink() || !stats.isFile()) throw new RunEvaluationStoreCorruptError(`${label} must be a regular non-symlink file`);
  return identityFromStats(stats);
}
async function readRegularFileNoFollowStandalone(path: string, label: string): Promise<string> {
  const before = await inspectRegularFile(path, label);
  const handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const opened = identityFromStats(await handle.stat({ bigint: true }));
    if (!sameIdentity(before, opened)) throw new RunEvaluationStoreCorruptError(`${label} changed before no-follow open`);
    const text = await handle.readFile("utf8");
    const after = await inspectRegularFile(path, label);
    if (!sameIdentity(opened, after)) throw new RunEvaluationStoreCorruptError(`${label} changed while being read`);
    return text;
  } finally { await handle.close(); }
}
async function fsyncExactDirectory(path: string, expected: PathIdentity): Promise<void> {
  await assertExactDirectory(path, expected, "directory fsync target");
  const handle = await open(path, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
  try { await handle.sync(); } finally { await handle.close(); }
  await assertExactDirectory(path, expected, "directory fsync target");
}
function lockClaimPath(rootDirectory: string, pid: number, processStartIdentity: string, token: string): string {
  const startTicks = processStartIdentity.slice("linux-proc:".length);
  if (!Number.isSafeInteger(pid) || pid <= 0 || !/^\d+$/.test(startTicks)) throw new RunEvaluationStoreCorruptError("Cannot name a lock claim without an exact process identity");
  return join(rootDirectory, `${LOCK_CLAIM_PREFIX}${pid}.${startTicks}.${token.replaceAll("-", "")}`);
}
function parseLockClaimName(name: string): { pid: number; processStartIdentity: string } | null {
  const match = /^telemetry\.lock\.claim\.([1-9][0-9]*)\.([0-9]+)\.[0-9a-f]{32}$/.exec(name);
  if (!match) return null;
  const pid = Number(match[1]);
  if (!Number.isSafeInteger(pid)) return null;
  return { pid, processStartIdentity: `linux-proc:${match[2]}` };
}
function parseLockOwner(raw: string): LockOwnerV1 {
  let value: any;
  try { value = parseStrictJson(raw); } catch (error) { throw new RunEvaluationStoreCorruptError("Evaluation lock owner metadata is not strict JSON", error); }
  const keys = ["kind", "lockDirectoryIdentityHash", "metadataHash", "ownerTokenHash", "pid", "processStartIdentity", "rootIdentityHash", "schemaVersion"];
  if (!value || typeof value !== "object" || Array.isArray(value) || Object.keys(value).sort().join(",") !== keys.sort().join(",")) throw new RunEvaluationStoreCorruptError("Evaluation lock owner metadata has an invalid closed shape");
  if (value.schemaVersion !== 1 || value.kind !== "run_evaluation_store_lock_owner" || !Number.isSafeInteger(value.pid) || value.pid <= 0 || typeof value.processStartIdentity !== "string" || !/^linux-proc:[0-9]+$/.test(value.processStartIdentity)) throw new RunEvaluationStoreCorruptError("Evaluation lock owner metadata has an invalid process identity");
  for (const key of ["ownerTokenHash", "rootIdentityHash", "lockDirectoryIdentityHash", "metadataHash"]) if (!HASH_PATTERN.test(value[key])) throw new RunEvaluationStoreCorruptError(`Evaluation lock owner metadata has invalid ${key}`);
  const { metadataHash, ...core } = value;
  if (metadataHash !== canonicalHash(core) || canonicalStringify(value) !== raw) throw new RunEvaluationStoreCorruptError("Evaluation lock owner metadata hash or canonical bytes do not match");
  return value as LockOwnerV1;
}
async function processStartIdentity(pid: number): Promise<string | null> {
  try {
    const text = await readFile(`/proc/${pid}/stat`, "utf8");
    const fields = text.slice(text.lastIndexOf(")") + 2).trim().split(/\s+/);
    if (!/^\d+$/.test(fields[19] ?? "")) throw new Error("missing process start ticks");
    return `linux-proc:${fields[19]}`;
  } catch (error: any) {
    if (error?.code === "ENOENT" || error?.code === "ESRCH") return null;
    throw error;
  }
}
async function requireOwnProcessStartIdentity(): Promise<string> {
  const identity = await processStartIdentity(process.pid);
  if (identity === null) throw new RunEvaluationStoreCorruptError("Cannot prove the executing process identity");
  return identity;
}
async function processIdentityStatus(pid: number, expected: string): Promise<"live" | "dead" | "reused" | "ambiguous"> {
  try {
    const actual = await processStartIdentity(pid);
    if (actual === null) return "dead";
    return actual === expected ? "live" : "reused";
  } catch { return "ambiguous"; }
}
function assertHash(value: string, label: string): void { if (!HASH_PATTERN.test(value)) throw new Error(`${label} must be a SHA-256 hash`); }

export const RUN_EVALUATION_INDEX_SCHEMA_HASH_V1 = canonicalHash(JSON.parse(JSON.stringify(RunEvaluationIndexV1Schema)));
export { RunEvaluationEnvelopeV1Schema, RunObservationAccumulatorV1Schema };
