import { createHash, randomUUID } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import { access, link, mkdir, open, readFile, readdir, rename, rm } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { canonicalHash, canonicalStringify, parseStrictJson, utcTimestampOrderValue } from "./common.ts";
import { dagRunSnapshotHash, parseDagRunStateV1, validateDagRunStateShapeV1, type DagRunStateV1, type DagRunValidationContextV1 } from "./run-state.ts";
import { reduceDagRunV1, type DagRunInputV1, type DagRunReducerResultV1 } from "./reducer.ts";

export interface DagRunStoreLockIdentityV1 {
  lockIdentity: string;
  ownerTokenHash: string;
  sessionId: string;
  pid: number;
  processStartIdentity: string;
  acquiredAt: string;
}
export interface DagRunStoreDeadOwnerProofV1 {
  expectedLockMetadataHash: string;
  processDisposition: "dead";
  observationHash: string;
  observedProcessDisposition: "dead_missing" | "dead_reused";
  observedAt: string;
}
export interface DagRunStoreOptionsV1 {
  failpoint?: (point: "after_snapshot_temp_sync" | "after_snapshot_rename" | "after_archive" | "after_recovery_intent" | "after_stale_lock_quarantine" | "after_replacement_lock" | "after_lock_release_rename" | "after_immutable_link" | "after_snapshot_read") => Promise<void> | void;
}
export interface DagRunStoreMutationV1 {
  input: DagRunInputV1;
  context: DagRunValidationContextV1;
  lock: DagRunStoreLockIdentityV1;
}

export class DagRunStoreLockedError extends Error {
  readonly lock: unknown;
  constructor(lock: unknown) { super(`DAG_RUN_LOCKED: ${JSON.stringify(lock)}`); this.name = "DagRunStoreLockedError"; this.lock = lock; }
}
export class DagRunStoreCorruptError extends Error {
  readonly causeValue: unknown;
  constructor(message: string, causeValue?: unknown) { super(message); this.name = "DagRunStoreCorruptError"; this.causeValue = causeValue; }
}

export class DagRunSnapshotStoreV1 {
  readonly rootDirectory: string;
  readonly runId: string;
  readonly options: DagRunStoreOptionsV1;
  readonly runDirectory: string;
  readonly statePath: string;
  readonly snapshotsDirectory: string;
  readonly factsDirectory: string;
  readonly corruptFactsDirectory: string;
  readonly lockDirectory: string;
  readonly lockMetadataPath: string;
  readonly lockRecoveryDirectory: string;
  readonly lockRecoveryMetadataPath: string;
  readonly ownerRecoveryIntentPath: string;
  readonly quarantinedLocksDirectory: string;

  constructor(rootDirectory: string, runId: string, options: DagRunStoreOptionsV1 = {}) {
    if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(runId) || runId === "." || runId === "..") throw new Error("Store runId must be one safe path segment");
    this.rootDirectory = resolve(rootDirectory);
    this.runId = runId;
    this.options = options;
    this.runDirectory = join(this.rootDirectory, runId);
    this.statePath = join(this.runDirectory, "run-state.json");
    this.snapshotsDirectory = join(this.runDirectory, "snapshots");
    this.factsDirectory = join(this.runDirectory, "facts");
    this.corruptFactsDirectory = join(this.runDirectory, "corrupt-facts");
    this.lockDirectory = join(this.runDirectory, "conductor.lock");
    this.lockMetadataPath = join(this.lockDirectory, "owner.json");
    this.lockRecoveryDirectory = join(this.runDirectory, "lock-recovery.lock");
    this.lockRecoveryMetadataPath = join(this.lockRecoveryDirectory, "owner.json");
    this.ownerRecoveryIntentPath = join(this.runDirectory, "owner-recovery-intent.json");
    this.quarantinedLocksDirectory = join(this.runDirectory, "quarantined-locks");
  }

  async initialize(state: DagRunStateV1, context: DagRunValidationContextV1, lock: DagRunStoreLockIdentityV1): Promise<void> {
    await this.ensureDirectories();
    await this.verifyContextFacts(context);
    if (await exists(this.statePath)) {
      const existing = await this.read(context);
      if (existing.revision !== 0 || existing.snapshotHash !== state.snapshotHash || canonicalStringify(existing) !== canonicalStringify(state)) throw new Error(`Dag run already exists: ${this.runId}`);
    }
    await this.prepareInitializationLock();
    await this.withLock(lock, async () => {
      if (await exists(this.statePath)) {
        const existing = await this.read(context);
        if (existing.revision === 0 && existing.snapshotHash === state.snapshotHash && canonicalStringify(existing) === canonicalStringify(state)) return;
        throw new Error(`Dag run already exists with different genesis: ${this.runId}`);
      }
      const canonical = parseDagRunStateV1(canonicalStringify(state), await this.contextWithReferencedFacts(state, context));
      if (canonical.runId !== this.runId) throw new Error("Snapshot runId does not match store namespace");
      if (canonical.revision !== 0 || canonical.previousSnapshotHash !== null) throw new Error("Run initialization requires revision zero with no predecessor");
      if (canonical.owner.sessionId !== null || canonical.owner.ownerEpoch !== 0) throw new Error("Run initialization requires a detached epoch-zero owner");
      await this.archiveSnapshot(canonical);
      await this.options.failpoint?.("after_archive");
      await this.writeSnapshot(canonical);
    });
  }

  async read(context: DagRunValidationContextV1, stabilityRetry = 0): Promise<DagRunStateV1> {
    await this.verifyContextFacts(context);
    let raw: string;
    try { raw = await readFile(this.statePath, "utf8"); }
    catch (error) { throw new DagRunStoreCorruptError(`Cannot read DAG run state ${this.statePath}`, error); }
    try {
      const shapeValue = parseStrictJson(raw);
      const shape = validateDagRunStateShapeV1(shapeValue);
      if (!shape.ok) throw new Error(`snapshot schema invalid: ${shape.issues.map(({ path, message }) => `${path} ${message}`).join("; ")}`);
      if (canonicalStringify(shapeValue) !== raw) throw new Error("snapshot bytes are not canonical JSON");
      const effectiveContext = await this.contextWithReferencedFacts(shape.value!, context);
      const state = parseDagRunStateV1(raw, effectiveContext);
      if (state.runId !== this.runId) throw new Error("Snapshot runId does not match store namespace");
      await this.options.failpoint?.("after_snapshot_read");
      const currentArchivePath = join(this.snapshotsDirectory, `${state.snapshotHash.slice("sha256:".length)}.json`);
      if (await readFile(currentArchivePath, "utf8") !== raw) throw new Error("current snapshot archive is missing or conflicts with published state bytes");
      if (state.revision > 0) {
        const previousPath = join(this.snapshotsDirectory, `${state.previousSnapshotHash!.slice("sha256:".length)}.json`);
        const previousValue = parseStrictJson(await readFile(previousPath, "utf8"));
        const previousShape = validateDagRunStateShapeV1(previousValue);
        if (!previousShape.ok) throw new Error(`archived predecessor schema invalid: ${previousShape.issues.map(({ path, message }) => `${path} ${message}`).join("; ")}`);
        const previous = previousShape.value!;
        if (previous.snapshotHash !== dagRunSnapshotHash(previous) || previous.snapshotHash !== state.previousSnapshotHash || previous.revision !== state.revision - 1 || previous.runId !== state.runId || previous.runNonce !== state.runNonce || previous.identity.planHash !== state.identity.planHash) throw new Error("snapshot predecessor hash/revision/identity chain does not resolve exactly");
        const ownerSlots = Object.values(state.idempotencySlots).filter((slot) => slot.appliedRevision === state.revision && ["attach_owner", "transfer_owner", "release_owner"].includes(slot.inputType));
        if (ownerSlots.length > 1) throw new Error("snapshot revision contains conflicting owner mutations");
        if (["attach_owner", "transfer_owner"].includes(ownerSlots[0]?.inputType ?? "")) {
          const ownership = effectiveContext.facts[state.owner.ownershipReceipt!] as any;
          if (!ownership || ownership.priorSessionId !== previous.owner.sessionId || ownership.priorPid !== previous.owner.pid || ownership.priorProcessStartIdentity !== previous.owner.processStartIdentity || ownership.priorLockIdentity !== previous.owner.lockIdentity || state.owner.ownerEpoch !== previous.owner.ownerEpoch + 1) throw new Error("owner attach does not bind the exact archived predecessor owner and next fencing epoch");
        } else if (ownerSlots[0]?.inputType === "release_owner") {
          if (previous.owner.sessionId === null || state.owner.sessionId !== null || state.owner.ownerEpoch !== previous.owner.ownerEpoch || state.owner.lastReleaseCommandId !== ownerSlots[0].commandId || state.owner.lastReleasePayloadHash !== ownerSlots[0].payloadHash) throw new Error("owner release does not bind the exact archived predecessor and release command");
        } else if (canonicalHash(state.owner) !== canonicalHash(previous.owner)) throw new Error("owner projection changed without an exact owner reducer slot");
      }
      return state;
    } catch (error) {
      const latest = await readFile(this.statePath, "utf8").catch(() => null);
      if (latest !== null && latest !== raw) {
        if (stabilityRetry < 3) return this.read(context, stabilityRetry + 1);
        throw new DagRunStoreLockedError({ reason: "snapshot changed repeatedly during stable read" });
      }
      throw new DagRunStoreCorruptError(`Invalid DAG run state ${this.statePath}`, error);
    }
  }

  async mutate({ input, context, lock }: DagRunStoreMutationV1): Promise<DagRunReducerResultV1> {
    await this.ensureDirectories();
    return this.withLock(lock, async () => {
      const current = await this.read(context);
      let effectiveContext = await this.contextWithReferencedFacts(current, context);
      if (input.type === "quarantine_fact") {
        const factRef = (input.payload as any).quarantine.fact;
        const storedFact = await this.readImmutableFact(factRef.hash).catch((error) => { throw new DagRunStoreCorruptError(`Quarantined fact ${factRef.hash} is not durably stored`, error); });
        if (Buffer.byteLength(canonicalStringify(storedFact)) !== factRef.bytes || (storedFact as any)?.hash !== factRef.hash || (storedFact as any)?.kind !== factRef.kind) throw new DagRunStoreCorruptError(`Quarantined fact ${factRef.hash} metadata or byte size does not match its immutable reference`);
        effectiveContext = { ...effectiveContext, facts: { ...effectiveContext.facts, [factRef.hash]: storedFact as any } };
      }
      if (input.type === "attach_owner" || input.type === "transfer_owner") {
        const duplicatePreview = reduceDagRunV1(current, input, effectiveContext);
        if (duplicatePreview.accepted && duplicatePreview.duplicate) return duplicatePreview;
        const payload = input.payload as any;
        const payloadMatchesHeldLock = payload.lockIdentity === lock.lockIdentity && payload.ownerTokenHash === lock.ownerTokenHash && payload.sessionId === lock.sessionId && payload.pid === lock.pid && payload.processStartIdentity === lock.processStartIdentity;
        const heldByCurrentOwner = current.owner.lockIdentity === lock.lockIdentity && current.owner.ownerTokenHash === lock.ownerTokenHash && current.owner.sessionId === lock.sessionId && current.owner.pid === lock.pid && current.owner.processStartIdentity === lock.processStartIdentity;
        if (input.type === "transfer_owner" ? !heldByCurrentOwner : !payloadMatchesHeldLock) throw new DagRunStoreLockedError({ reason: "owner attach must be executed by absent successor; transfer requires exact current process" });
        const exactCurrentOwner = payloadMatchesHeldLock && heldByCurrentOwner;
        if (input.type === "attach_owner" && current.owner.sessionId !== null) {
          return { accepted: false, code: "STALE_OWNER", message: "attached owner replacement requires explicit transfer or proven-dead recovery", currentRevision: current.revision, blockerIds: [] };
        }
        if (current.owner.sessionId !== null && !exactCurrentOwner && payload.priorOwnerDisposition !== "same_manager") {
          return { accepted: false, code: "STALE_OWNER", message: "attached owner takeover requires direct-transfer or proven-dead recovery protocol", currentRevision: current.revision, blockerIds: [] };
        }
      } else if (current.owner.lockIdentity !== lock.lockIdentity || current.owner.ownerTokenHash !== lock.ownerTokenHash || current.owner.sessionId !== lock.sessionId || current.owner.pid !== lock.pid || current.owner.processStartIdentity !== lock.processStartIdentity) {
        return {
          accepted: false, code: "STALE_OWNER", message: "held lock identity/token does not match current conductor owner",
          currentRevision: current.revision, blockerIds: Object.values(current.blockers).filter(({ active }) => active).map(({ blockerId }) => blockerId).sort(),
        };
      }
      const reduced = reduceDagRunV1(current, input, effectiveContext);
      if (!reduced.accepted || reduced.duplicate) return reduced;
      await this.archiveSnapshot(current);
      await this.archiveSnapshot(reduced.state);
      await this.options.failpoint?.("after_archive");
      await this.writeSnapshot(reduced.state);
      await this.pruneSnapshotArchives(reduced.state);
      return reduced;
    });
  }

  async putImmutableFact(value: unknown): Promise<{ hash: string; path: string; bytes: number }> {
    const normalized = parseStrictJson(canonicalStringify(value));
    const text = canonicalStringify(normalized);
    const hash = immutableFactHash(normalized);
    const path = join(this.factsDirectory, `${hash.slice("sha256:".length)}.json`);
    await this.ensureDirectories();
    if (await exists(path)) {
      const existing = await readFile(path, "utf8");
      if (existing !== text) throw new DagRunStoreCorruptError(`Immutable fact collision at ${path}`);
      await fsyncDirectory(this.factsDirectory);
      return { hash, path, bytes: Buffer.byteLength(text) };
    }
    try { await durablePublishImmutable(path, text, async () => this.options.failpoint?.("after_immutable_link")); }
    catch (error: any) {
      if (error?.code !== "EEXIST") throw error;
      const existing = await readFile(path, "utf8");
      if (existing !== text) throw new DagRunStoreCorruptError(`Immutable fact race conflict at ${path}`);
    }
    await fsyncDirectory(this.factsDirectory);
    return { hash, path, bytes: Buffer.byteLength(text) };
  }

  async quarantineCorruptImmutableFact(hash: string, quarantinedAt: string): Promise<{ fact: unknown; ref: { kind: "corrupt_fact"; schemaVersion: 1; id: string; hash: string; bytes: number; mediaType: "application/json"; sensitivity: "internal"; retention: "run"; locator: null }; rawPath: string }> {
    if (!/^sha256:[0-9a-f]{64}$/.test(hash)) throw new Error("Invalid immutable fact hash");
    await this.ensureDirectories();
    const source = join(this.factsDirectory, `${hash.slice("sha256:".length)}.json`);
    const rawPath = join(this.corruptFactsDirectory, `${hash.slice("sha256:".length)}.raw`);
    if (await exists(source)) {
      try { await this.readImmutableFact(hash); throw new Error("Immutable fact is canonical and does not require corruption quarantine"); }
      catch (error) { if (error instanceof Error && error.message.includes("does not require")) throw error; }
      if (await exists(rawPath)) throw new DagRunStoreCorruptError("Both corrupt fact source and raw quarantine identity exist");
      await rename(source, rawPath);
      await fsyncDirectory(this.factsDirectory);
      await fsyncDirectory(this.corruptFactsDirectory);
    }
    if (!await exists(rawPath)) throw new DagRunStoreCorruptError(`Corrupt immutable fact ${hash} is unavailable for quarantine`);
    const raw = await readFile(rawPath);
    const rawBytesHash = `sha256:${createHash("sha256").update(raw).digest("hex")}`;
    const core = { kind: "corrupt_fact" as const, claimedHash: hash, rawBytesHash, rawBytes: raw.byteLength, quarantinedAt, rawPathIdentityHash: canonicalHash({ runId: this.runId, claimedHash: hash, rawBytesHash }) };
    const fact = { ...core, hash: canonicalHash(core) };
    const stored = await this.putImmutableFact(fact);
    return { fact, ref: { kind: "corrupt_fact", schemaVersion: 1, id: `corrupt-${hash.slice("sha256:".length, "sha256:".length + 16)}`, hash: stored.hash, bytes: stored.bytes, mediaType: "application/json", sensitivity: "internal", retention: "run", locator: null }, rawPath };
  }

  async readImmutableFact(hash: string): Promise<unknown> {
    if (!/^sha256:[0-9a-f]{64}$/.test(hash)) throw new Error("Invalid immutable fact hash");
    const path = join(this.factsDirectory, `${hash.slice("sha256:".length)}.json`);
    const raw = await readFile(path, "utf8");
    const value = parseStrictJson(raw);
    if (immutableFactHash(value) !== hash || canonicalStringify(value) !== raw) throw new DagRunStoreCorruptError(`Immutable fact hash/canonicalization mismatch at ${path}`);
    return value;
  }

  async reattachAfterDeadOwner(
    proof: DagRunStoreDeadOwnerProofV1,
    input: DagRunInputV1,
    context: DagRunValidationContextV1,
    newLock: DagRunStoreLockIdentityV1,
    verifyDeadOwnerProof: (proof: DagRunStoreDeadOwnerProofV1, lock: unknown) => Promise<boolean>,
  ): Promise<{ result: DagRunReducerResultV1; quarantinedLockPath: string }> {
    const proofKeys = Object.keys(proof as unknown as Record<string, unknown>).sort();
    if (JSON.stringify(proofKeys) !== JSON.stringify(["expectedLockMetadataHash", "observationHash", "observedAt", "observedProcessDisposition", "processDisposition"].sort()) || !/^sha256:[0-9a-f]{64}$/.test(proof.expectedLockMetadataHash) || !/^sha256:[0-9a-f]{64}$/.test(proof.observationHash) || proof.processDisposition !== "dead" || !["dead_missing", "dead_reused"].includes(proof.observedProcessDisposition) || !Number.isFinite(utcTimestampOrderValue(proof.observedAt))) throw new Error("Dead-owner proof must be one closed canonical process observation");
    if (input.type !== "attach_owner" || (input.payload as any).lockIdentity !== newLock.lockIdentity) throw new Error("Dead-owner recovery requires an exact attach-owner input for the replacement lock");
    await assertExecutingProcessIdentity(newLock);
    const ownership = context.facts[(input.payload as any).ownershipReceipt] as any;
    let recoveryContext = context;
    if (!ownership || ownership.kind !== "ownership" || ownership.priorObservationHash !== proof.observationHash || ownership.disposition !== "dead" || ownership.successorLockIdentity !== newLock.lockIdentity || ownership.successorSessionId !== newLock.sessionId || ownership.successorPid !== newLock.pid || ownership.successorProcessStartIdentity !== newLock.processStartIdentity) throw new Error("Dead-owner recovery must bind one exact immutable ownership/death fact");
    await this.ensureDirectories();
    await this.verifyContextFacts(context);
    await this.acquireRecoveryLock(newLock);
    let replacementLockHeld = false;
    let recoverySucceeded = false;
    try {
      let staleLock: unknown = null;
      let independentlyVerified = false;
      if (await exists(this.ownerRecoveryIntentPath)) {
        const intent = parseStrictJson(await readFile(this.ownerRecoveryIntentPath, "utf8")) as any;
        const samePriorDeath = intent.proof.expectedLockMetadataHash === proof.expectedLockMetadataHash && intent.proof.processDisposition === proof.processDisposition && intent.proof.observedAt === proof.observedAt;
        if (!samePriorDeath) {
          const current = await this.read(context);
          if (current.owner.lockIdentity === (intent.staleLock as any)?.lockIdentity) throw new Error("Owner-recovery retry conflicts with active durable recovery intent");
          await rm(this.ownerRecoveryIntentPath, { force: true });
          await fsyncDirectory(this.runDirectory);
          staleLock = null;
        } else staleLock = intent.staleLock;
      }
      if (!staleLock) {
        staleLock = await this.inspectLock();
        const current = await this.read(context);
        if (!staleLock && current.owner.sessionId !== null) staleLock = dagRunStoreLockIdentityFromOwner(current.owner);
        if (!staleLock || canonicalHash(staleLock) !== proof.expectedLockMetadataHash) throw new Error("Dead-owner proof does not bind exact current owner/lock metadata");
        if (current.owner.lockIdentity !== (staleLock as any).lockIdentity || current.owner.pid !== (staleLock as any).pid || current.owner.processStartIdentity !== (staleLock as any).processStartIdentity) throw new Error("Stale lock does not match the current conductor snapshot identity");
        if (!await verifyDeadOwnerProof(proof, staleLock)) throw new Error("Prior conductor death is not proven by the independent observer");
        independentlyVerified = true;
        await durablePublishImmutable(this.ownerRecoveryIntentPath, canonicalStringify({ proof, staleLock }));
        await this.options.failpoint?.("after_recovery_intent");
      }
      assertLockIdentityShape(staleLock as DagRunStoreLockIdentityV1);
      const recoveryStateBeforeQuarantine = await this.read(context);
      if (recoveryStateBeforeQuarantine.owner.lockIdentity !== (staleLock as any).lockIdentity || recoveryStateBeforeQuarantine.owner.pid !== (staleLock as any).pid || recoveryStateBeforeQuarantine.owner.processStartIdentity !== (staleLock as any).processStartIdentity) throw new Error("Durable recovery intent already committed or no longer binds the current owner; a fresh proof is required");
      const directDisposition = await processIdentityDisposition(staleLock as DagRunStoreLockIdentityV1);
      const exactObservationHash = dagRunStoreProcessObservationHash(staleLock as DagRunStoreLockIdentityV1, directDisposition, proof.observedAt);
      if (proof.processDisposition !== "dead" || !directDisposition.startsWith("dead") || proof.observedProcessDisposition !== directDisposition || proof.observationHash !== exactObservationHash || (!independentlyVerified && !await verifyDeadOwnerProof(proof, staleLock))) throw new Error("Prior conductor death is not proven by exact process identity and immutable observation");
      const processObservation = dagRunStoreProcessObservationFact(staleLock as DagRunStoreLockIdentityV1, directDisposition, proof.observedAt);
      await this.putImmutableFact(processObservation);
      recoveryContext = { ...context, facts: { ...context.facts, [processObservation.hash]: processObservation } };
      await mkdir(this.quarantinedLocksDirectory, { recursive: true });
      const quarantinePath = join(this.quarantinedLocksDirectory, proof.expectedLockMetadataHash.slice("sha256:".length));
      const visibleLock = await this.inspectLock();
      if (visibleLock && canonicalHash(visibleLock) === canonicalHash(newLock)) replacementLockHeld = true;
      else if (visibleLock && canonicalHash(visibleLock) === canonicalHash(staleLock)) {
        if (await exists(quarantinePath)) throw new Error("Both stale and quarantined lock identities exist");
        await rename(this.lockDirectory, quarantinePath);
        await fsyncDirectory(this.quarantinedLocksDirectory);
        await fsyncDirectory(this.runDirectory);
        await this.options.failpoint?.("after_stale_lock_quarantine");
      } else if (visibleLock) {
        const priorReplacementDisposition = await processIdentityDisposition(visibleLock as DagRunStoreLockIdentityV1);
        if (!priorReplacementDisposition.startsWith("dead")) throw new Error("Conductor lock changed during owner recovery and replacement death is not proven");
        const abandonedReplacementPath = join(this.quarantinedLocksDirectory, `replacement-${canonicalHash(visibleLock).slice("sha256:".length)}`);
        if (await exists(abandonedReplacementPath)) throw new DagRunStoreCorruptError("Both abandoned replacement lock and its quarantine identity exist");
        await rename(this.lockDirectory, abandonedReplacementPath);
        await fsyncDirectory(this.quarantinedLocksDirectory);
        await fsyncDirectory(this.runDirectory);
      }
      if (!await exists(quarantinePath)) {
        if (visibleLock) throw new Error("Durable stale-lock quarantine is missing");
        await mkdir(quarantinePath, { mode: 0o700 });
        await fsyncDirectory(this.quarantinedLocksDirectory);
      }
      if (!await exists(join(quarantinePath, "recovery.json"))) await durablePublishImmutable(join(quarantinePath, "recovery.json"), canonicalStringify(proof));
      await fsyncDirectory(quarantinePath);
      if (!replacementLockHeld) { await this.publishLock(newLock, true); replacementLockHeld = true; await this.options.failpoint?.("after_replacement_lock"); }
      const current = await this.read(context);
      const reduced = reduceDagRunV1(current, input, await this.contextWithReferencedFacts(current, recoveryContext));
      if (!reduced.accepted) throw new Error(`Replacement owner attach rejected: ${reduced.code}: ${reduced.message}`);
      if (!reduced.duplicate) {
        await this.archiveSnapshot(current);
        await this.archiveSnapshot(reduced.state);
        await this.writeSnapshot(reduced.state);
        await this.pruneSnapshotArchives(reduced.state);
      }
      recoverySucceeded = true;
      return { result: reduced, quarantinedLockPath: quarantinePath };
    } finally {
      if (replacementLockHeld) await this.releaseDirectoryLock(this.lockDirectory, this.lockMetadataPath, newLock, "conductor").catch(() => undefined);
      if (recoverySucceeded) await rm(this.ownerRecoveryIntentPath, { force: true }).catch(() => undefined);
      await this.releaseDirectoryLock(this.lockRecoveryDirectory, this.lockRecoveryMetadataPath, newLock, "recovery").catch(() => undefined);
      await fsyncDirectory(this.runDirectory).catch(() => undefined);
    }
  }

  async inspectLock(): Promise<unknown | null> { return this.inspectMetadata(this.lockMetadataPath); }

  private async prepareInitializationLock(): Promise<void> {
    if (!await exists(this.lockDirectory)) return;
    const existing = await this.inspectLock();
    if (!existing || (existing as any).corrupt) throw new DagRunStoreCorruptError("Initialization lock exists without complete canonical owner metadata");
    assertLockIdentityShape(existing as DagRunStoreLockIdentityV1);
    const disposition = await processIdentityDisposition(existing as DagRunStoreLockIdentityV1);
    if (!disposition.startsWith("dead")) throw new DagRunStoreLockedError({ initialization: true, metadata: existing, disposition });
    const quarantinePath = join(this.quarantinedLocksDirectory, `initialization-${canonicalHash(existing).slice("sha256:".length)}`);
    if (await exists(quarantinePath)) throw new DagRunStoreCorruptError("Both stale initialization lock and quarantine identity exist");
    await rename(this.lockDirectory, quarantinePath);
    await fsyncDirectory(this.quarantinedLocksDirectory);
    await fsyncDirectory(this.runDirectory);
  }

  private async inspectMetadata(path: string): Promise<unknown | null> {
    try { return parseStrictJson(await readFile(path, "utf8")); }
    catch (error: any) { return error?.code === "ENOENT" ? null : { corrupt: true, message: error instanceof Error ? error.message : String(error) }; }
  }

  private async acquireRecoveryLock(lock: DagRunStoreLockIdentityV1): Promise<void> {
    try { await this.publishDirectoryLock(this.lockRecoveryDirectory, lock, ".recovery-lock"); return; }
    catch (error) { if (!(error instanceof DagRunStoreLockedError)) throw error; }
    const existing = await this.inspectMetadata(this.lockRecoveryMetadataPath);
    if (!existing || (existing as any).corrupt) throw new DagRunStoreLockedError({ recovery: true, metadata: existing });
    assertLockIdentityShape(existing as DagRunStoreLockIdentityV1);
    const disposition = await processIdentityDisposition(existing as DagRunStoreLockIdentityV1);
    if (!disposition.startsWith("dead")) throw new DagRunStoreLockedError({ recovery: true, metadata: existing, disposition });
    const quarantinePath = join(this.quarantinedLocksDirectory, `recovery-${canonicalHash(existing).slice("sha256:".length)}`);
    if (await exists(quarantinePath)) throw new DagRunStoreCorruptError("Recovery-lock quarantine identity already exists while stale lock is still visible");
    await rename(this.lockRecoveryDirectory, quarantinePath);
    await fsyncDirectory(this.quarantinedLocksDirectory);
    await fsyncDirectory(this.runDirectory);
    await this.publishDirectoryLock(this.lockRecoveryDirectory, lock, ".recovery-lock");
  }

  private async contextWithReferencedFacts(state: DagRunStateV1, context: DagRunValidationContextV1): Promise<DagRunValidationContextV1> {
    const hashes = new Set<string>();
    const semanticHashes = new Set<string>();
    const addSemantic = (hash: string) => { hashes.add(hash); semanticHashes.add(hash); };
    hashes.add(state.identity.reviewReceipt.hash);
    for (const receipt of state.identity.authorizationReceipts) hashes.add(receipt.hash);
    hashes.add(state.identity.authorizationSet.hash);
    hashes.add(state.freshness.receipt.hash);
    if (state.owner.ownershipReceipt) addSemantic(state.owner.ownershipReceipt);
    for (const effect of Object.values(state.effects)) if (effect.observationHash) addSemantic(effect.observationHash);
    for (const repository of Object.values(state.repositories)) { hashes.add(repository.observationReceipt); if (repository.workspace.observationReceipt) hashes.add(repository.workspace.observationReceipt); }
    for (const binding of Object.values(state.workerBindings)) { hashes.add(binding.configRef.hash); if (binding.resultHash) addSemantic(binding.resultHash); }
    for (const attempt of Object.values(state.stageAttempts)) { addSemantic(attempt.attemptInput.hash); if (attempt.workerResult) addSemantic(attempt.workerResult.hash); if (attempt.evidence) addSemantic(attempt.evidence.hash); }
    for (const item of Object.values(state.workItems)) { if (item.candidate) addSemantic(item.candidate.candidateHash); if (item.integrationReadyReceipt) addSemantic(item.integrationReadyReceipt); if (item.integrationReceipt) addSemantic(item.integrationReceipt); }
    for (const index of [state.evidenceIndex.stageAttemptInputs, state.evidenceIndex.workerResults, state.evidenceIndex.candidates, state.evidenceIndex.stageEvidence, state.evidenceIndex.checkDispositions, state.evidenceIndex.verifications, state.evidenceIndex.oracleAssertions, state.evidenceIndex.findings, state.evidenceIndex.findingResolutions, state.evidenceIndex.waivers, state.evidenceIndex.invalidations, state.evidenceIndex.adoptions, state.evidenceIndex.effectReconciliations, state.evidenceIndex.integrationReady, state.evidenceIndex.integrationReceipts, state.evidenceIndex.gateReceipts]) for (const reference of Object.values(index)) addSemantic(reference.hash);
    for (const reference of Object.values(state.evidenceIndex.stalenessReceipts)) hashes.add(reference.hash);
    for (const entry of Object.values(state.quarantine)) { addSemantic(entry.fact.hash); if (entry.adoptionReceipt) addSemantic(entry.adoptionReceipt); }
    const facts = { ...context.facts };
    for (const hash of hashes) {
      const stored = await this.readImmutableFact(hash).catch((error) => { throw new DagRunStoreCorruptError(`Snapshot referenced immutable fact ${hash} is unavailable`, error); });
      if (!(stored as any)?.hash || (stored as any).hash !== hash) throw new DagRunStoreCorruptError(`Snapshot referenced immutable fact ${hash} does not bind its own hash`);
      if (facts[hash] && canonicalStringify(facts[hash]) !== canonicalStringify(stored)) throw new DagRunStoreCorruptError(`Snapshot referenced immutable fact ${hash} conflicts with supplied validation context`);
      if (semanticHashes.has(hash)) facts[hash] = stored as any;
    }
    if (state.owner.ownershipReceipt) {
      const ownership = facts[state.owner.ownershipReceipt] as any;
      if (ownership?.priorObservationHash) {
        const observation = await this.readImmutableFact(ownership.priorObservationHash).catch((error) => { throw new DagRunStoreCorruptError(`Owner process observation ${ownership.priorObservationHash} is unavailable`, error); });
        if ((observation as any)?.kind !== "process_identity_observation" || (observation as any)?.hash !== ownership.priorObservationHash) throw new DagRunStoreCorruptError(`Owner process observation ${ownership.priorObservationHash} has invalid identity`);
        facts[ownership.priorObservationHash] = observation as any;
      }
    }
    const authorityReceipts = { ...(context.authorityReceipts ?? {}) };
    for (const entry of Object.values(state.quarantine)) {
      const stored = facts[entry.fact.hash] as any;
      if (stored?.kind !== entry.fact.kind || Buffer.byteLength(canonicalStringify(stored)) !== entry.fact.bytes) throw new DagRunStoreCorruptError(`Quarantine reference ${entry.quarantineId} does not match durable immutable fact metadata`);
      if (entry.adoptionReceipt) {
        const resolution = facts[entry.adoptionReceipt] as any;
        if (resolution?.kind !== "quarantine_resolution") throw new DagRunStoreCorruptError(`Quarantine adoption ${entry.quarantineId} lacks its immutable resolution`);
        const authority = await this.readImmutableFact(resolution.authorityReceiptHash).catch((error) => { throw new DagRunStoreCorruptError(`Quarantine adoption authority ${resolution.authorityReceiptHash} is unavailable`, error); });
        if ((authority as any)?.kind !== "quarantine_authority" || (authority as any)?.hash !== resolution.authorityReceiptHash) throw new DagRunStoreCorruptError(`Quarantine adoption authority ${resolution.authorityReceiptHash} has invalid identity`);
        if (authorityReceipts[resolution.authorityReceiptHash] && canonicalStringify(authorityReceipts[resolution.authorityReceiptHash]) !== canonicalStringify(authority)) throw new DagRunStoreCorruptError(`Quarantine adoption authority ${resolution.authorityReceiptHash} conflicts with supplied authority context`);
        authorityReceipts[resolution.authorityReceiptHash] = authority as any;
      }
    }
    return { ...context, facts, authorityReceipts };
  }

  private async verifyContextFacts(context: DagRunValidationContextV1): Promise<void> {
    for (const [hash, expected] of Object.entries({ ...context.facts, ...(context.authorityReceipts ?? {}) })) {
      const stored = await this.readImmutableFact(hash).catch((error) => { throw new DagRunStoreCorruptError(`Validation context fact ${hash} is not durably stored`, error); });
      if (canonicalHash(stored) !== canonicalHash(expected)) throw new DagRunStoreCorruptError(`Validation context fact ${hash} conflicts with immutable store content`);
    }
  }

  private async ensureDirectories(): Promise<void> {
    const rootWasMissing = !await exists(this.rootDirectory);
    await mkdir(this.rootDirectory, { recursive: true });
    if (rootWasMissing) await fsyncDirectory(dirname(this.rootDirectory));
    const runWasMissing = !await exists(this.runDirectory);
    await mkdir(this.runDirectory, { recursive: true, mode: 0o700 });
    if (runWasMissing) await fsyncDirectory(this.rootDirectory);
    for (const directory of [this.snapshotsDirectory, this.factsDirectory, this.corruptFactsDirectory, this.quarantinedLocksDirectory]) await mkdir(directory, { recursive: true, mode: 0o700 });
    await fsyncDirectory(this.runDirectory);
  }

  private async writeSnapshot(state: DagRunStateV1): Promise<void> {
    const text = canonicalStringify(state);
    const temp = join(this.runDirectory, `.run-state.${process.pid}.${randomUUID()}.tmp`);
    const handle = await open(temp, "wx", 0o600);
    try { await handle.writeFile(text, "utf8"); await handle.sync(); await this.options.failpoint?.("after_snapshot_temp_sync"); }
    finally { await handle.close(); }
    try { await rename(temp, this.statePath); await this.options.failpoint?.("after_snapshot_rename"); await fsyncDirectory(this.runDirectory); }
    catch (error) { await rm(temp, { force: true }).catch(() => undefined); throw error; }
  }

  private async pruneSnapshotArchives(state: DagRunStateV1): Promise<void> {
    const retained = new Set([state.snapshotHash, state.previousSnapshotHash].filter((hash): hash is string => Boolean(hash)).map((hash) => `${hash.slice("sha256:".length)}.json`));
    for (const name of await readdir(this.snapshotsDirectory)) if (/^[0-9a-f]{64}\.json$/.test(name) && !retained.has(name)) await rm(join(this.snapshotsDirectory, name), { force: true });
    await fsyncDirectory(this.snapshotsDirectory);
  }

  private async archiveSnapshot(state: DagRunStateV1): Promise<void> {
    const path = join(this.snapshotsDirectory, `${state.snapshotHash.slice("sha256:".length)}.json`);
    const text = canonicalStringify(state);
    if (await exists(path)) {
      if (await readFile(path, "utf8") !== text) throw new DagRunStoreCorruptError(`Snapshot archive collision at ${path}`);
      await fsyncDirectory(this.snapshotsDirectory);
      return;
    }
    try { await durablePublishImmutable(path, text, async () => this.options.failpoint?.("after_immutable_link")); }
    catch (error: any) {
      if (error?.code !== "EEXIST" || await readFile(path, "utf8") !== text) throw error;
      await fsyncDirectory(this.snapshotsDirectory);
    }
    await fsyncDirectory(this.snapshotsDirectory);
  }

  private async publishLock(lock: DagRunStoreLockIdentityV1, allowRecovery = false): Promise<void> {
    if (!allowRecovery && (await exists(this.lockRecoveryDirectory) || await exists(this.ownerRecoveryIntentPath))) throw new DagRunStoreLockedError({ recovery: true });
    await this.publishDirectoryLock(this.lockDirectory, lock, ".conductor-lock");
    if (!allowRecovery && (await exists(this.lockRecoveryDirectory) || await exists(this.ownerRecoveryIntentPath))) {
      const visible = await this.inspectLock();
      if (visible && canonicalHash(visible) === canonicalHash(lock)) await this.releaseDirectoryLock(this.lockDirectory, this.lockMetadataPath, lock, "conductor");
      throw new DagRunStoreLockedError({ recovery: true });
    }
  }

  private async publishDirectoryLock(directory: string, lock: DagRunStoreLockIdentityV1, prefix: string): Promise<void> {
    const tempDirectory = join(this.runDirectory, `${prefix}.${process.pid}.${randomUUID()}.tmp`);
    await mkdir(tempDirectory, { mode: 0o700 });
    try {
      await durableCreateExclusive(join(tempDirectory, "owner.json"), canonicalStringify(lock));
      await fsyncDirectory(tempDirectory);
      try { await rename(tempDirectory, directory); }
      catch (error: any) {
        if (await exists(directory)) throw new DagRunStoreLockedError(await this.inspectMetadata(join(directory, "owner.json")));
        throw error;
      }
      await fsyncDirectory(this.runDirectory);
    } finally { await rm(tempDirectory, { recursive: true, force: true }).catch(() => undefined); }
  }

  private async releaseDirectoryLock(directory: string, metadataPath: string, lock: DagRunStoreLockIdentityV1, label: string): Promise<void> {
    const visible = await this.inspectMetadata(metadataPath);
    if (!visible || canonicalHash(visible) !== canonicalHash(lock)) return;
    const retired = join(this.runDirectory, `.${label}-released-${canonicalHash(lock).slice("sha256:".length)}-${randomUUID()}`);
    await rename(directory, retired);
    await fsyncDirectory(this.runDirectory);
    await this.options.failpoint?.("after_lock_release_rename");
    await rm(retired, { recursive: true, force: true });
    await fsyncDirectory(this.runDirectory);
  }

  private async withLock<T>(lock: DagRunStoreLockIdentityV1, operation: () => Promise<T>): Promise<T> {
    await assertExecutingProcessIdentity(lock);
    await this.publishLock(lock);
    try {
      return await operation();
    } finally {
      await this.releaseDirectoryLock(this.lockDirectory, this.lockMetadataPath, lock, "conductor").catch(() => undefined);
      await fsyncDirectory(this.runDirectory).catch(() => undefined);
    }
  }
}

function immutableFactHash(value: unknown): string {
  if (value && typeof value === "object" && !Array.isArray(value) && Object.prototype.hasOwnProperty.call(value, "hash")) {
    const expected = (value as any).hash;
    const actual = canonicalHash(Object.fromEntries(Object.entries(value as Record<string, unknown>).filter(([key]) => key !== "hash")));
    if (expected !== actual) throw new DagRunStoreCorruptError("Immutable fact self-hash does not match canonical content");
    return actual;
  }
  return canonicalHash(value);
}

function assertLockIdentityShape(identity: DagRunStoreLockIdentityV1): void {
  const keys = Object.keys(identity as unknown as Record<string, unknown>).sort();
  const expectedKeys = ["lockIdentity", "ownerTokenHash", "sessionId", "pid", "processStartIdentity", "acquiredAt"].sort();
  if (JSON.stringify(keys) !== JSON.stringify(expectedKeys) || !/^sha256:[0-9a-f]{64}$/.test(identity.lockIdentity) || !/^sha256:[0-9a-f]{64}$/.test(identity.ownerTokenHash) || typeof identity.sessionId !== "string" || !identity.sessionId || !Number.isInteger(identity.pid) || identity.pid < 1 || typeof identity.processStartIdentity !== "string" || !identity.processStartIdentity || !Number.isFinite(utcTimestampOrderValue(identity.acquiredAt))) throw new DagRunStoreCorruptError("Process-shared lock identity is not closed and canonical");
}

export function dagRunStoreLockIdentityFromOwner(owner: DagRunStateV1["owner"]): DagRunStoreLockIdentityV1 {
  if (owner.sessionId === null || owner.ownerTokenHash === null || owner.pid < 1 || owner.processStartIdentity === null || owner.lockIdentity === null || owner.attachedAt === null) throw new Error("Attached owner identity is incomplete");
  return { lockIdentity: owner.lockIdentity, ownerTokenHash: owner.ownerTokenHash, sessionId: owner.sessionId, pid: owner.pid, processStartIdentity: owner.processStartIdentity, acquiredAt: owner.attachedAt };
}

export function dagRunStoreProcessObservationFact(identity: DagRunStoreLockIdentityV1, disposition: "dead_missing" | "dead_reused", observedAt: string): { kind: "process_identity_observation"; hash: string; lockMetadataHash: string; pid: number; processStartIdentity: string; disposition: "dead_missing" | "dead_reused"; observedAt: string } {
  assertLockIdentityShape(identity);
  const core = { kind: "process_identity_observation" as const, lockMetadataHash: canonicalHash(identity), pid: identity.pid, processStartIdentity: identity.processStartIdentity, disposition, observedAt };
  return { ...core, hash: canonicalHash(core) };
}

export function dagRunStoreProcessObservationHash(identity: DagRunStoreLockIdentityV1, disposition: "dead_missing" | "dead_reused", observedAt: string): string {
  return dagRunStoreProcessObservationFact(identity, disposition, observedAt).hash;
}

export async function createDagRunStoreDeadOwnerProofV1(identity: DagRunStoreLockIdentityV1, observedAt: string): Promise<DagRunStoreDeadOwnerProofV1> {
  assertLockIdentityShape(identity);
  if (!Number.isFinite(utcTimestampOrderValue(observedAt))) throw new Error("Process observation time must be a valid UTC timestamp");
  const disposition = await processIdentityDisposition(identity);
  if (!disposition.startsWith("dead")) throw new Error(`Process identity is not proven dead: ${disposition}`);
  return { expectedLockMetadataHash: canonicalHash(identity), processDisposition: "dead", observationHash: dagRunStoreProcessObservationHash(identity, disposition, observedAt), observedProcessDisposition: disposition, observedAt };
}

async function assertExecutingProcessIdentity(identity: DagRunStoreLockIdentityV1): Promise<void> {
  assertLockIdentityShape(identity);
  if (identity.pid !== process.pid || await processIdentityDisposition(identity) !== "live") throw new DagRunStoreLockedError({ reason: "lock identity does not match the executing PID/start identity", pid: process.pid });
}

async function processIdentityDisposition(identity: DagRunStoreLockIdentityV1): Promise<"live" | "dead_missing" | "dead_reused" | "ambiguous"> {
  if (!Number.isInteger(identity.pid) || identity.pid < 1 || !identity.processStartIdentity.startsWith("linux-proc:")) return "ambiguous";
  try { process.kill(identity.pid, 0); }
  catch (error: any) { return error?.code === "ESRCH" ? "dead_missing" : "ambiguous"; }
  try {
    const text = await readFile(`/proc/${identity.pid}/stat`, "utf8");
    const close = text.lastIndexOf(")");
    if (close < 0) return "ambiguous";
    const startTicks = text.slice(close + 2).trim().split(/\s+/)[19];
    if (!startTicks) return "ambiguous";
    return `linux-proc:${startTicks}` === identity.processStartIdentity ? "live" : "dead_reused";
  } catch { return "ambiguous"; }
}

async function durablePublishImmutable(path: string, text: string, afterLink?: () => Promise<void> | void): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  const temp = join(dirname(path), `.${randomUUID()}.immutable.tmp`);
  const handle = await open(temp, fsConstants.O_WRONLY | fsConstants.O_CREAT | fsConstants.O_EXCL, 0o600);
  try { await handle.writeFile(text, "utf8"); await handle.sync(); }
  finally { await handle.close(); }
  try { await link(temp, path); await afterLink?.(); }
  finally { await rm(temp, { force: true }); }
  await fsyncDirectory(dirname(path));
}
async function durableCreateExclusive(path: string, text: string): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  const handle = await open(path, fsConstants.O_WRONLY | fsConstants.O_CREAT | fsConstants.O_EXCL, 0o600);
  try { await handle.writeFile(text, "utf8"); await handle.sync(); }
  finally { await handle.close(); }
}
async function fsyncDirectory(path: string): Promise<void> {
  const handle = await open(path, fsConstants.O_RDONLY);
  try { await handle.sync(); } finally { await handle.close(); }
}
async function exists(path: string): Promise<boolean> {
  try { await access(path, fsConstants.F_OK); return true; } catch { return false; }
}
