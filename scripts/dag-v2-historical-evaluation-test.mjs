import assert from "node:assert/strict";
import { lstat, mkdtemp, mkdir, readFile, readdir, readlink, rename, rm, symlink, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import dagWorkflow from "../extensions/dag-workflow/index.ts";
import { canonicalHash, canonicalStringify } from "../extensions/dag-workflow/dag-runtime/common.ts";
import { RunEvaluationStoreV1 } from "../extensions/dag-workflow/dag-runtime/evaluation-store.ts";
import {
  RUN_EVALUATION_CLOCK_POLICY_HASH_V1, RUN_EVALUATION_PROFILE_V1,
  accumulatorClockV1, accumulatorDerivedMetricsV1, buildRunEvaluationEnvelopeV1,
  createRunObservationAccumulatorV1, cutoffIdentityHashV1, measuredMetric, validateRunEvaluationEnvelopeV1,
} from "../extensions/dag-workflow/dag-runtime/evaluation.ts";

const AT = "2026-08-05T00:00:00.000Z";
async function retainedEnvelope(store) {
  const hash = label => canonicalHash({ fixture: label });
  const identity = { projectIdentityHash: hash("project"), runIdentityHash: hash("run"), runNonceHash: hash("nonce"), planHash: hash("plan") };
  const creditContext = { acceptedIntegrationLineages: [], actionableFindingDispositions: [] };
  const accumulator = createRunObservationAccumulatorV1({
    identity: { ...identity, evaluationProfileHash: RUN_EVALUATION_PROFILE_V1.profileHash, clockPolicyHash: RUN_EVALUATION_CLOCK_POLICY_HASH_V1 },
    source: { revision: 0, snapshotHash: hash("snapshot"), observedAt: AT, clockEpochHash: hash("epoch"), monotonicTickMs: 0 }, creditContext,
  });
  await store.initialize();
  await store.writeAccumulator(accumulator, null);
  const source = { revision: 0, snapshotHash: accumulator.source.snapshotHash, accumulatorHash: accumulator.accumulatorHash,
    reviewReceiptHash: hash("review"), authorizationReceiptHash: hash("authorization"), freshnessReceiptHash: hash("freshness") };
  const cutoff = { kind: "terminal", class: "plan_complete", cutoffAt: AT, checkpointIdentityHash: null };
  cutoff.cutoffIdentityHash = cutoffIdentityHashV1({ identity, source, cutoff });
  const count = () => measuredMetric("count", 0, null, 1);
  const derived = accumulatorDerivedMetricsV1(accumulator, { serialPolicy: false, rightCensored: false });
  const envelope = buildRunEvaluationEnvelopeV1({
    schemaVersion: 1, kind: "run_evaluation_envelope", canonicalization: "jcs-v1", evaluationProfile: { ...RUN_EVALUATION_PROFILE_V1 },
    identity, source, creditContext, cutoff, supersedesEnvelopeHash: null, serialPolicy: false,
    sourceHashes: { authorization: [source.authorizationReceiptHash], stageEvidence: [hash("stage")], workerResults: [hash("worker")],
      findingsAndResolutions: [hash("finding")], effectReconciliation: [hash("effect")], verification: [source.reviewReceiptHash],
      integration: [hash("integration")], otherRequired: [source.accumulatorHash, source.freshnessReceiptHash].sort() },
    attribution: { creditedOperations: [], falseIndependenceIncidents: [] }, clock: accumulatorClockV1(accumulator),
    coverage: { status: "measured", sourceRevisionCount: 1, observedRevisionCount: accumulator.coverage.observedRevisionCount,
      missingRevisionCount: 0, droppedRevisionCount: 0, censoredIntervalCount: 0, observerFailureCount: 0 },
    invariants: { snapshotAndHashes: "pass", planSourceJoins: "pass", authorizationAndScope: "pass", idempotencyAndStaleAdvancement: "pass",
      effectsReconciled: "pass", integrationExact: "pass", completionExact: "pass" },
    metrics: { ...derived, outcomes: { accepted: count(), integrated: count() }, attempts: { attempts: count(), retries: count(), backEdges: count() },
      findings: { ...derived.findings, total: count(), disposed: count() }, integration: { conflicts: count(), invalidations: count(), reconciledEffects: count() },
      humanAttention: { ...derived.humanAttention, decisions: count() },
      modelUsage: { inputTokens: measuredMetric("token", 0), outputTokens: measuredMetric("token", 0), cacheReadTokens: measuredMetric("token", 0),
        cacheWriteTokens: measuredMetric("token", 0), inferenceRequests: count(), reportedCost: measuredMetric("provider_reported_cost", 0) } },
    postRunPulse: Object.fromEntries(["confidenceFinalState", "cognitiveEffort", "interruptionBurden"].map(key => [key, { status: "not_observed", value: null }])),
  });
  assert.equal(validateRunEvaluationEnvelopeV1(envelope).ok, true);
  await store.publishEnvelope(envelope, AT);
  const retained = await store.readEnvelope(envelope.envelopeHash);
  assert.equal(canonicalStringify(retained), canonicalStringify(envelope));
  return retained;
}

// Include directories, exact bytes, symlink targets and inode/mtime/ctime identities;
// reads may change atime, but even create-and-delete or same-byte replacement must fail.
async function snapshot(root) {
  const entries = {};
  async function visit(path, name) {
    const stat = await lstat(path, { bigint: true });
    const entry = { mode: String(stat.mode), dev: String(stat.dev), ino: String(stat.ino), nlink: String(stat.nlink),
      mtime: String(stat.mtimeNs), ctime: String(stat.ctimeNs) };
    entries[name] = entry;
    if (stat.isSymbolicLink()) entry.target = await readlink(path);
    else if (stat.isDirectory()) for (const child of (await readdir(path)).sort()) await visit(join(path, child), `${name}/${child}`);
    else entry.bytes = (await readFile(path)).toString("base64");
  }
  await visit(root, ".");
  return entries;
}

const tools = new Map();
const pi = {
  registerTool(tool) { assert(!tools.has(tool.name)); tools.set(tool.name, tool); },
  registerCommand() {}, on() {}, getActiveTools() { return []; }, setActiveTools() {},
  getAllTools() { return [...tools.values()]; },
  appendEntry() { assert.fail("history must not append session authority"); },
  sendMessage() { assert.fail("history must not dispatch messages"); },
};
const role = process.env.PI_DAG_WORKER_ROLE;
try { delete process.env.PI_DAG_WORKER_ROLE; dagWorkflow(pi); }
finally { if (role !== undefined) process.env.PI_DAG_WORKER_ROLE = role; }
const tool = tools.get("dag_history_v1");
assert(tool, "actual extension must register dag_history_v1");
const call = async (root, id) => (await tool.execute("historical-evaluation-test", { kind: "evaluation", id }, undefined, undefined, { cwd: root })).details;
const tests = [];
const test = (name, run) => tests.push([name, run]);
async function fixture(run) {
  const root = await mkdtemp(join(tmpdir(), "dag-v2-historical-evaluation-"));
  try { await run(root); } finally { await rm(root, { recursive: true, force: true }); }
}
async function preserved(root, operation) {
  const before = await snapshot(root);
  try { return await operation(); } finally { assert.deepEqual(await snapshot(root), before, "whole fixture path/byte identity changed"); }
}

test("registered history reads a canonical published V1 envelope without changing any path or byte", () => fixture(async root => {
  const store = new RunEvaluationStoreV1(root), envelope = await retainedEnvelope(store);
  await preserved(root, async () => {
    assert.deepEqual(await call(root, envelope.envelopeHash), envelope);
    assert.deepEqual(await call(root, envelope.envelopeHash), envelope);
    assert.equal(await readFile(join(store.envelopesDirectory, `${envelope.envelopeHash.slice(7)}.json`), "utf8"), canonicalStringify(envelope));
  });
  // Exact envelope reads do not depend on an index, or acquire/repair a lock.
  await unlink(store.indexPath);
  await mkdir(store.lockDirectory);
  await writeFile(join(store.lockDirectory, "owner.json"), "not a valid lock owner\n");
  await preserved(root, async () => assert.deepEqual(await call(root, envelope.envelopeHash), envelope));
}));

for (const missing of [".ai", "dag-evaluations-v1", "accumulators", "envelopes"]) {
  test(`missing ${missing} fails closed without initialization`, () => fixture(async root => {
    const store = new RunEvaluationStoreV1(root), envelope = await retainedEnvelope(store);
    const path = missing === ".ai" ? store.aiDirectory : missing === "dag-evaluations-v1" ? store.rootDirectory : join(store.rootDirectory, missing);
    await rm(path, { recursive: true });
    await preserved(root, () => assert.rejects(call(root, envelope.envelopeHash), { code: "ENOENT" }));
  }));
}

for (const alias of ["project", ".ai", "dag-evaluations-v1", "accumulators", "envelopes", "envelope"]) {
  test(`symlink ${alias} fails closed without following or changing the alias`, () => fixture(async root => {
    const project = join(root, "project"); await mkdir(project);
    const store = new RunEvaluationStoreV1(project), envelope = await retainedEnvelope(store);
    const path = alias === "project" ? project : alias === ".ai" ? store.aiDirectory : alias === "dag-evaluations-v1" ? store.rootDirectory
      : alias === "envelope" ? join(store.envelopesDirectory, `${envelope.envelopeHash.slice(7)}.json`) : join(store.rootDirectory, alias);
    const moved = join(root, "retained-original"); await rename(path, moved); await symlink(moved, path);
    await preserved(root, () => assert.rejects(call(project, envelope.envelopeHash), /non-symlink|Invalid evaluation envelope/));
  }));
}

for (const corruption of ["invalid-json", "noncanonical", "hash-mismatch", "missing-envelope"]) {
  test(`${corruption} envelope fails closed without resealing or repair`, () => fixture(async root => {
    const store = new RunEvaluationStoreV1(root), envelope = await retainedEnvelope(store);
    const path = join(store.envelopesDirectory, `${envelope.envelopeHash.slice(7)}.json`);
    if (corruption === "missing-envelope") await unlink(path);
    else await writeFile(path, corruption === "invalid-json" ? "{broken" : corruption === "noncanonical" ? `${canonicalStringify(envelope)}\n`
      : canonicalStringify({ ...envelope, serialPolicy: !envelope.serialPolicy }));
    await preserved(root, () => assert.rejects(call(root, envelope.envelopeHash), /Invalid evaluation envelope/));
  }));
}

test("read-only binding detects replaced directory identity and never rebinds it", () => fixture(async root => {
  const store = new RunEvaluationStoreV1(root), envelope = await retainedEnvelope(store), reader = new RunEvaluationStoreV1(root);
  await preserved(root, async () => { await reader.attachReadOnly(); await reader.attachReadOnly(); assert.deepEqual(await reader.readEnvelope(envelope.envelopeHash), envelope); });
  await rename(store.envelopesDirectory, `${store.envelopesDirectory}.retained`); await mkdir(store.envelopesDirectory);
  await preserved(root, async () => {
    await assert.rejects(reader.readEnvelope(envelope.envelopeHash), /device\/inode identity changed/);
    await assert.rejects(reader.attachReadOnly(), /device\/inode identity changed/);
  });
}));

let passed = 0;
for (const [name, run] of tests) {
  await run(); console.log(`ok ${++passed} - ${name}`);
}
console.log(`dag V2 historical evaluation tests passed: ${passed}`);
