import { readFile, writeFile } from "node:fs/promises";
import { canonicalHash, DagRunSnapshotStoreV1, DagRunStoreLockedError } from "../../extensions/dag-workflow/dag-runtime/index.ts";

const [root, runId, contextPath, inputPath, lockPath, crashPoint, mode = "mutate", proofPath] = process.argv.slice(2);
if (!root || !runId || !contextPath || !inputPath || !lockPath) throw new Error("usage: dag-store-child <root> <runId> <context> <input> <lock> [crash-point] [mutate|recover] [proof]");
let context = JSON.parse(await readFile(contextPath, "utf8"));
let input = JSON.parse(await readFile(inputPath, "utf8"));
let lock = JSON.parse(await readFile(lockPath, "utf8"));
const store = new DagRunSnapshotStoreV1(root, runId, { failpoint: async (point) => { if (point === crashPoint) process.exit(86); } });
try {
  if (mode === "initialize-auto") {
    const text = await readFile(`/proc/${process.pid}/stat`, "utf8");
    const processStartIdentity = `linux-proc:${text.slice(text.lastIndexOf(")") + 2).trim().split(/\s+/)[19]}`;
    lock = { ...lock, pid: process.pid, processStartIdentity };
    await writeFile(lockPath, JSON.stringify(lock));
  }
  if (mode === "attach-auto" || mode === "transfer-auto" || mode === "transfer-cas-auto") {
    const text = await readFile(`/proc/${process.pid}/stat`, "utf8");
    const processStartIdentity = `linux-proc:${text.slice(text.lastIndexOf(")") + 2).trim().split(/\s+/)[19]}`;
    const current = await store.read(context);
    lock = { ...lock, pid: process.pid, processStartIdentity };
    const disposition = current.owner.sessionId === null ? "absent" : "same_manager";
    const lineageHash = disposition === "same_manager" ? canonicalHash({ priorSessionId: current.owner.sessionId, priorOwnerTokenHash: current.owner.ownerTokenHash, successorSessionId: lock.sessionId, manager: "dag-store-child" }) : null;
    const ownershipInput = { kind: "ownership", runId: current.runId, runNonce: current.runNonce, priorSessionId: current.owner.sessionId, priorOwnerTokenHash: current.owner.ownerTokenHash, priorPid: current.owner.pid, priorProcessStartIdentity: current.owner.processStartIdentity, priorLockIdentity: current.owner.lockIdentity, priorAttachedAt: current.owner.attachedAt, disposition, priorObservationHash: null, successorSessionId: lock.sessionId, successorPid: lock.pid, successorProcessStartIdentity: lock.processStartIdentity, successorLockIdentity: lock.lockIdentity, lineageHash };
    const ownership = { ...ownershipInput, hash: canonicalHash(ownershipInput) };
    await store.putImmutableFact(ownership);
    const payload = { ownerTokenHash: lock.ownerTokenHash, sessionId: lock.sessionId, pid: lock.pid, processStartIdentity: lock.processStartIdentity, lockIdentity: lock.lockIdentity, ownershipReceipt: ownership.hash, priorOwnerDisposition: disposition };
    input = { ...input, kind: disposition === "same_manager" ? "command" : "observation", type: disposition === "same_manager" ? "transfer_owner" : "attach_owner", payload, payloadHash: canonicalHash(payload), ...(mode === "transfer-cas-auto" ? {} : { expectedRevision: current.revision, expectedSnapshotHash: current.snapshotHash, ownerEpoch: current.owner.ownerEpoch }) };
    context = { ...context, facts: { ...context.facts, [ownership.hash]: ownership } };
    await writeFile(lockPath, JSON.stringify(lock));
    await writeFile(inputPath, JSON.stringify(input));
  }
  if (mode === "recover-auto") {
    const text = await readFile(`/proc/${process.pid}/stat`, "utf8");
    const processStartIdentity = `linux-proc:${text.slice(text.lastIndexOf(")") + 2).trim().split(/\s+/)[19]}`;
    const current = await store.read(context);
    lock = { ...lock, pid: process.pid, processStartIdentity };
    const ownershipInput = { kind: "ownership", runId: current.runId, runNonce: current.runNonce, priorSessionId: current.owner.sessionId, priorOwnerTokenHash: current.owner.ownerTokenHash, priorPid: current.owner.pid, priorProcessStartIdentity: current.owner.processStartIdentity, priorLockIdentity: current.owner.lockIdentity, priorAttachedAt: current.owner.attachedAt, disposition: "dead", priorObservationHash: JSON.parse(await readFile(proofPath, "utf8")).observationHash, successorSessionId: lock.sessionId, successorPid: lock.pid, successorProcessStartIdentity: lock.processStartIdentity, successorLockIdentity: lock.lockIdentity, lineageHash: null };
    const ownership = { ...ownershipInput, hash: canonicalHash(ownershipInput) };
    await store.putImmutableFact(ownership);
    const payload = { ownerTokenHash: lock.ownerTokenHash, sessionId: lock.sessionId, pid: lock.pid, processStartIdentity: lock.processStartIdentity, lockIdentity: lock.lockIdentity, ownershipReceipt: ownership.hash, priorOwnerDisposition: "dead" };
    input = { ...input, payload, payloadHash: canonicalHash(payload), expectedRevision: current.revision, expectedSnapshotHash: current.snapshotHash, ownerEpoch: current.owner.ownerEpoch };
    context = { ...context, facts: { ...context.facts, [ownership.hash]: ownership } };
  }
  const result = mode === "publish-fact"
    ? (await store.putImmutableFact(input), { accepted: true, duplicate: false })
    : mode === "initialize-auto"
      ? (await store.initialize(input, context, lock), { accepted: true, duplicate: false })
    : mode === "recover" || mode === "recover-auto"
      ? (await store.reattachAfterDeadOwner(JSON.parse(await readFile(proofPath, "utf8")), input, context, lock, async () => true)).result
      : await store.mutate({ input, context, lock });
  process.stdout.write(JSON.stringify({ accepted: result.accepted, duplicate: result.accepted ? result.duplicate : false, code: result.accepted ? null : result.code }));
} catch (error) {
  if (error instanceof DagRunStoreLockedError) process.stdout.write(JSON.stringify({ accepted: false, duplicate: false, code: "LOCKED" }));
  else throw error;
}
