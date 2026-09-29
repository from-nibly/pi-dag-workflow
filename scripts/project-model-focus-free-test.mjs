import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { ProjectModelDomain } from "../extensions/dag-workflow/project-model/domain.ts";
import { validateProjectModel, semanticHash } from "../extensions/dag-workflow/project-model/model.ts";
import { FocusSessionStore } from "../extensions/dag-workflow/project-model/sessions.ts";

const root = await mkdtemp(join(tmpdir(), "model-focus-free-"));
const scope = { workstreamIds: [] };
const value = (title) => ({ title, body: `${title} body`, sourceRefs: [], relationships: [] });
const point = (key, direction) => ({ key, title: key, context: `Resolve ${key} independently.`, purpose: "decision", question: `Choose ${key}?`, options: [{ key: "yes", label: "Yes", description: "Use this direction", ...(direction ? { direction } : {}) }] });
const child = (path, args) => new Promise((resolve, reject) => {
  const processHandle = spawn(process.execPath, [path, ...args], { stdio: ["ignore", "pipe", "pipe"] });
  let output = "";
  processHandle.stderr.on("data", (data) => { output += data; });
  processHandle.on("error", reject);
  processHandle.on("exit", (code) => code === 0 ? resolve() : reject(new Error(output)));
});
try {
  const domain = new ProjectModelDomain(root);
  await domain.models.initialize("no-focus", "No focus");
  await assert.rejects(domain.update(undefined, { add: [] }), /Explicit workstreamIds/);
  await domain.update(scope, { add: ["alpha", "beta"].map((key) => ({ collection: "workstreams", key, value: value(key) })) });
  await domain.update({ workstreamIds: ["WS-alpha"] }, { add: [{ collection: "discoveries", key: "finding", value: value("finding") }] });
  assert.deepEqual((await domain.models.load()).discoveries[0].scope, { kind: "workstreams", workstreamIds: ["WS-alpha"] });
  await domain.update(scope, { patch: [{ id: "DISC-finding", changes: { body: "Updated finding" } }] });
  await domain.update(scope, { removeIds: ["DISC-finding"] });
  await domain.recordDirection(scope, { directions: [{ collection: "intents", key: "goal", value: { ...value("goal"), kind: "outcome" } }], currentUnderstanding: { body: "Contextual user intent, not hash consent.", sourceObjectIds: ["INT-goal"] } });
  await domain.recordDirection(scope, { directions: [{ collection: "intents", id: "INT-goal", value: { body: "Revised contextual direction" } }] });
  let model = await domain.models.load();
  assert.equal(model.intents[0].state, "accepted");
  assert.equal(model.intents[0].acceptance, undefined);
  assert.deepEqual(model.project.currentUnderstanding.sourceObjects, [{ id: "INT-goal" }]);
  assert.deepEqual(validateProjectModel(model), []);
  assert.deepEqual(await readdir(root), ["project-model"]);

  const a = await domain.createReview({ workstreamIds: ["WS-alpha"] }, { title: "Alpha", points: [point("a"), point("unanswered")] });
  const b = await domain.createReview({ workstreamIds: ["WS-beta"] }, { title: "Beta", points: [point("b")] });
  assert.deepEqual((await domain.pendingReviews(scope)).map((r) => r.reviewId).sort(), [a.review.id, b.review.id].sort());
  assert.deepEqual((await domain.pendingReviews({ workstreamIds: ["WS-alpha"] })).map((r) => r.reviewId), [a.review.id]);
  await assert.rejects(domain.createReview(scope, { id: a.review.id, title: "Duplicate", points: [point("dup")] }), /already exists/);
  await assert.rejects(domain.resolveReview({ expectedRevision: 0, outcomes: [] }), /Exact reviewId/);
  await assert.rejects(domain.resolveReview({ reviewId: a.review.id, outcomes: [] }), /expectedRevision/);
  const results = await Promise.allSettled([0, 1].map(() => new ProjectModelDomain(root).resolveReview({ reviewId: a.review.id, expectedRevision: 0, outcomes: [{ pointId: "point-a", action: "accept", optionId: "option-yes" }] })));
  assert.equal(results.filter((r) => r.status === "fulfilled").length, 1);
  assert.match(String(results.find((r) => r.status === "rejected").reason), /revision conflict/);
  assert.deepEqual((await domain.context(scope, { view: "review", reviewId: a.review.id })).review.outcomes.map((r) => r.pointId), ["point-a"]);
  assert.equal((await domain.context(scope, { view: "review", reviewId: b.review.id })).review.revision, 0);

  // Independent review creation from separate processes is serialized by the real model lock.
  const program = join(root, "review-child.mjs");
  const domainUrl = new URL("../extensions/dag-workflow/project-model/domain.ts", import.meta.url).href;
  await writeFile(program, `import {ProjectModelDomain} from ${JSON.stringify(domainUrl)}; const [root,key]=process.argv.slice(2); const d=new ProjectModelDomain(root); await d.createReview({workstreamIds:[]},{title:key,points:[{title:key,context:key,purpose:"awareness"}]});`);
  await Promise.all(Array.from({ length: 6 }, (_, i) => child(program, [root, `parallel-${i}`])));
  assert.equal((await domain.pendingReviews(scope)).length, 8);
  const resolveProgram = join(root, "resolve-child.mjs");
  await writeFile(resolveProgram, `import {ProjectModelDomain} from ${JSON.stringify(domainUrl)}; const [root,key]=process.argv.slice(2); const d=new ProjectModelDomain(root); await d.resolveReview({reviewId:'review-parallel-'+key,expectedRevision:0,outcomes:[{pointId:'point-parallel-'+key,action:'accept'}]});`);
  await Promise.all(Array.from({ length: 6 }, (_, i) => child(resolveProgram, [root, String(i)])));
  assert.equal((await domain.pendingReviews(scope)).length, 2);
  assert.equal((await domain.models.load()).project.reviews.filter((r) => r.status === "resolved").length, 6);

  // Failure cannot publish an outcome without its model change, or vice versa.
  const atomic = await domain.createReview(scope, { title: "Atomic", points: [point("atomic", { collection: "decisions", key: "atomic", value: value("Atomic decision") })] });
  const before = await readFile(domain.models.path, "utf8");
  await assert.rejects(domain.resolveReview({ reviewId: atomic.review.id, expectedRevision: 0, outcomes: [{ pointId: "point-atomic", action: "accept", optionId: "option-yes" }], update: { patch: [{ id: "missing", changes: { title: "invalid" } }] } }), /Unknown model object/);
  assert.equal(await readFile(domain.models.path, "utf8"), before);
  await domain.resolveReview({ reviewId: atomic.review.id, expectedRevision: 0, outcomes: [{ pointId: "point-atomic", action: "accept", optionId: "option-yes" }] });
  model = await domain.models.load();
  assert.equal(model.decisions.find((object) => object.id === "DEC-atomic").acceptance, undefined);
  assert.equal(model.project.reviews.find((review) => review.id === atomic.review.id).status, "resolved");
  // Unchosen alternatives must not veto current direction; actual cycles still fail.
  await domain.recordDirection(scope, { directions: ["a", "b"].map(key => ({ collection: "decisions", key, value: value(key) })) });
  const alternative = await domain.createReview(scope, { title: "Alternative", points: [point("reverse", { collection: "decisions", id: "DEC-b", value: { relationships: [{ kind: "supersedes", targetId: "DEC-a" }] } })] });
  await domain.recordDirection(scope, { directions: [{ collection: "decisions", id: "DEC-a", value: { relationships: [{ kind: "supersedes", targetId: "DEC-b" }] } }] });
  assert.deepEqual(validateProjectModel(await domain.models.load()), []);
  const beforeCycle = await readFile(domain.models.path, "utf8");
  await assert.rejects(domain.recordDirection(scope, { directions: [{ collection: "decisions", id: "DEC-b", value: { relationships: [{ kind: "supersedes", targetId: "DEC-a" }] } }] }), /supersession cycle/);
  await assert.rejects(domain.resolveReview({ reviewId: alternative.review.id, expectedRevision: 0, outcomes: [{ pointId: "point-reverse", action: "accept", optionId: "option-yes" }] }), /supersession cycle/);
  await assert.rejects(domain.createReview(scope, { title: "Invalid alternative", points: [point("cycle", { collection: "decisions", id: "DEC-b", value: { relationships: [{ kind: "supersedes", targetId: "DEC-a" }] } })] }), /supersession cycle/);
  assert.equal(await readFile(domain.models.path, "utf8"), beforeCycle);

  // Historical outcome inputs retain partial direction and dead identities, not malformed structure.
  const historical = await domain.createReview(scope, { title: "History", points: [point("history")] });
  await domain.resolveReview({ reviewId: historical.review.id, expectedRevision: 0, outcomes: [{ pointId: "point-history", action: "modify", direction: { collection: "decisions", id: "DEC-atomic", value: { body: "Changed later" } } }] });
  const historyModel = await domain.models.load();
  const historyOutcome = m => m.project.reviews.find(r => r.id === historical.review.id).outcomes[0];
  historyOutcome(historyModel).direction.id = "DEC-no-longer-live";
  assert.deepEqual(validateProjectModel(historyModel), []);
  historyModel.project.revision++;
  await domain.models.write(historyModel);
  assert.equal(historyOutcome(await domain.models.load()).direction.id, "DEC-no-longer-live");
  for (const direction of [{ collection: "not-a-collection", value: 17 }, { collection: "decisions", id: 17 }, { collection: "decisions", id: "DEC-gone", state: "impossible" }, { collection: "decisions", id: "DEC-gone", value: { relationships: [{ kind: "impossible", targetId: "DEC-gone" }] } }]) {
    const invalid = structuredClone(historyModel); historyOutcome(invalid).direction = direction; invalid.project.revision++;
    assert(validateProjectModel(invalid).some(e => /direction/.test(e)));
    await assert.rejects(domain.models.write(invalid), /direction/);
    const validBytes = await readFile(domain.models.path, "utf8");
    await writeFile(domain.models.path, JSON.stringify(invalid));
    await assert.rejects(domain.models.load(), /direction/);
    await writeFile(domain.models.path, validBytes);
  }
  const missingApplied = structuredClone(historyModel);
  delete missingApplied.project.reviews.find(r => r.id === atomic.review.id).outcomes[0].direction;
  assert(validateProjectModel(missingApplied).some(e => /requires applied direction/.test(e)));
  for (const action of ["modify", "accept"]) {
    const invalid = structuredClone(historyModel); const outcome = historyOutcome(invalid);
    outcome.action = action; delete outcome.direction; delete outcome.optionId;
    assert(validateProjectModel(invalid).some(e => /requires/.test(e)));
  }

  await domain.update(scope, { specViews: [{ id: "SPEC-current", kind: "spec", path: "spec/current.md", title: "Current", sections: [{ id: "meaning", title: "Meaning", objectIds: ["INT-goal", "DEC-atomic", "DEC-a", "DEC-b"] }] }] });
  await domain.cutover(scope);
  assert.equal((await domain.specs({ action: "check" })).ok, true);
  assert.match(await readFile(join(root, "spec/current.md"), "utf8"), /Revised contextual direction/);

  // Historical data is readable without upgrading or activating the old singleton.
  const timestamp = new Date().toISOString();
  const legacyReview = { id: "review-old", title: "Old review", createdAt: timestamp, points: [{ id: "point-old", title: "Old", context: "Old question", purpose: "decision", question: "Keep it?", objectRefs: [], options: [{ id: "option-old", label: "Old", description: "Old option", semanticHash: "historical-cache-id" }] }] };
  const legacy = { schemaVersion: 1, id: "focus-old", title: "Old focus", status: "active", workstreamIds: [], createdAt: timestamp, updatedAt: timestamp, activeReview: legacyReview };
  const sessions = new FocusSessionStore(root);
  await mkdir(sessions.dir, { recursive: true });
  const legacyBytes = JSON.stringify(legacy);
  await writeFile(sessions.path(legacy.id), legacyBytes);
  assert.equal((await domain.pendingReviews(scope)).find((review) => review.reviewId === legacyReview.id).readOnly, true);
  assert.deepEqual((await domain.context(scope, { view: "legacy_review", legacyFocusId: legacy.id, reviewId: legacyReview.id })).review, legacyReview);
  await assert.rejects(domain.context(scope, { view: "legacy_review", legacyFocusId: legacy.id, reviewId: "review-other" }), /address not found/);
  await domain.update(scope, { add: [{ collection: "discoveries", value: value("No legacy mutation") }] });
  assert.equal(await readFile(sessions.path(legacy.id), "utf8"), legacyBytes);
  await writeFile(sessions.path("focus-corrupt"), "not json");
  await domain.recordDirection(scope, { directions: [{ collection: "intents", id: "INT-goal", value: { body: "Direction survives corrupt old routing" } }] });
  model = await domain.models.load();
  const historic = structuredClone(model);
  historic.intents[0].acceptance = { mode: "direct_direction", actor: "user", acceptedAt: timestamp, contentHash: semanticHash("intents", historic.intents[0]), interactionRef: "historical-record" };
  historic.intents[0].body = "Historical receipt no longer matches; it is not an active consent gate.";
  historic.project.currentUnderstanding.sourceObjects[0].semanticHash = "historical-source-identity";
  assert.deepEqual(validateProjectModel(historic), []);
  assert.equal(sessions.write, undefined);
  console.log("Focus-free model CRUD/direction, independent reviews/CAS/process concurrency, atomic outcomes, projections and legacy readers OK");
} finally { await rm(root, { recursive: true, force: true }); }
