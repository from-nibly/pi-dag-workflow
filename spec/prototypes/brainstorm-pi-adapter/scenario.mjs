import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { BRAINSTORM_TOOLS, BrainstormPiAdapter, FakePi, FakeUi, createSnapshot, listSnapshots, snapshotPath } from "./adapter.mjs";

const root = await mkdtemp(join(tmpdir(), "brainstorm-pi-adapter-"));
try {
  const pi = new FakePi();
  pi.activeTools.add("other_extension_tool");
  const domain = createDomain();
  let rendererMode = "fail";
  const renderer = {
    async present(review) {
      if (rendererMode === "fail") throw new Error("Lavish unavailable");
      return { selected: review.points[0].proposalIds[0], comment: "Use the selected contract" };
    },
  };
  const adapter = new BrainstormPiAdapter({ root, domain, renderer });
  adapter.register(pi);

  assert(BRAINSTORM_TOOLS.every((name) => pi.tools.has(name)), "registers exactly the accepted brainstorm tool names");
  assert(BRAINSTORM_TOOLS.every((name) => !pi.activeTools.has(name)), "brainstorm tools start inactive");
  assert(pi.activeTools.has("other_extension_tool"), "registration preserves tools owned by other extensions");
  assert(!pi.tools.has("dag_grillme_get_state") && !pi.commands.has("grillme"), "prototype has no GrillMe registration");

  const headless = { cwd: root, hasUI: false, mode: "print", ui: new FakeUi() };
  await assert.rejects(pi.runCommand("dag", "brainstorm", headless), /requires --continue <id> or --new <id>/);
  await pi.runCommand("dag", "brainstorm --new alpha authorization delivery", headless);
  assert.equal(adapter.activeId, "alpha");
  assert(BRAINSTORM_TOOLS.every((name) => pi.activeTools.has(name)), "command activates all five tools");
  assert(pi.activeTools.has("other_extension_tool"), "activation does not clobber unrelated tools");
  assert.match(pi.sentUserMessages.at(-1), /Orientation:/);
  assert.match(await pi.systemPrompt(), /STRUCTURED_BRAINSTORM_GUIDANCE/);

  const context = await pi.callTool("dag_brainstorm_context", { id: "alpha", view: "orientation" }, headless);
  assert.equal(context.details.revision, 0);
  assert(!context.content[0].text.includes("authorization delivery"), "compact receipt does not echo stored/authored prose");

  await Promise.all([
    pi.callTool("dag_brainstorm_update", { id: "alpha", changes: { counterDelta: 1 }, understanding: "Initial causal model" }, headless),
    pi.callTool("dag_brainstorm_update", { id: "alpha", changes: { counterDelta: 1 } }, headless),
  ]);
  let state = await loadState(root, "alpha");
  assert.equal(state.counter, 2, "per-snapshot mutation queue prevents lost concurrent updates");
  assert.equal(state.currentUnderstanding.body, "Initial causal model");

  const reviewInput = {
    id: "alpha",
    key: "delivery-review",
    presentation: "lavish",
    points: [{ id: "RP-owner", questionIds: ["Q-owner"], proposalIds: ["P-requester"] }],
  };
  const failedRender = await pi.callTool("dag_brainstorm_review", reviewInput, headless);
  assert.equal(failedRender.details.rendererError, "Lavish unavailable");
  state = await loadState(root, "alpha");
  assert.equal(state.reviews.length, 1, "renderer failure leaves the semantic review active");

  rendererMode = "success";
  const rendered = await pi.callTool("dag_brainstorm_review", { ...reviewInput, replace: true }, headless);
  assert.equal(rendered.details.submission.comment, "Use the selected contract");
  state = await loadState(root, "alpha");
  assert.equal(JSON.stringify(state).includes("Use the selected contract"), false, "unprocessed renderer submission is not persisted");

  await pi.callTool("dag_brainstorm_resolve_review", {
    id: "alpha",
    reviewId: "R-delivery-review",
    outcomes: [{ pointId: "RP-owner", proposalId: "P-requester" }],
    understanding: "Requester ownership keeps installation authority with the target environment.",
  }, headless);
  state = await loadState(root, "alpha");
  assert.equal(state.reviews.length, 0);
  assert.equal(state.decisions.length, 1);
  assert.match(state.currentUnderstanding.body, /installation authority/);

  const prepared = await pi.callTool("dag_brainstorm_promote", { id: "alpha", action: "prepare", decisionIds: [state.decisions[0].id] }, headless);
  assert.equal(prepared.details.action, "prepare");
  await mkdir(join(root, "spec", "delivery"), { recursive: true });
  await writeFile(join(root, "spec", "delivery", "spec.md"), "# Delivery\n", "utf8");
  await pi.callTool("dag_brainstorm_promote", {
    id: "alpha",
    action: "record",
    targets: [{ path: "spec/delivery/spec.md", decisionIds: [state.decisions[0].id] }],
  }, headless);
  state = await loadState(root, "alpha");
  assert.equal(state.promotions.length, 1, "promotion record follows agent-authored spec edit");

  await pi.runCommand("dag", "plan", headless);
  assert(BRAINSTORM_TOOLS.every((name) => !pi.activeTools.has(name)), "another DAG workflow suspends brainstorm tools");
  assert(pi.activeTools.has("other_extension_tool"), "suspension removes only brainstorm tools");
  assert.doesNotMatch(await pi.systemPrompt(), /STRUCTURED_BRAINSTORM_GUIDANCE/);
  await assert.rejects(pi.callTool("dag_brainstorm_context", { id: "alpha" }, headless), /Inactive tool/);

  await createSnapshot(root, "beta", "Beta");
  await writeFile(join(root, ".ai", "brainstorm", "broken.json"), "{bad json", "utf8");
  const candidates = await listSnapshots(root);
  assert.deepEqual(candidates.map(({ id }) => id), ["alpha", "beta"], "malformed disposable snapshots are excluded from resume choices");

  const interactive = { cwd: root, hasUI: true, mode: "tui", ui: new FakeUi(["Resume", "beta"]) };
  await pi.runCommand("dag", "brainstorm", interactive);
  assert.equal(adapter.activeId, "beta", "interactive command asks resume/new and then selects among multiple snapshots");
  await assert.rejects(pi.runCommand("dag", "brainstorm --continue missing", interactive), /snapshot not found/);

  console.log("Brainstorm Pi adapter prototype OK: command selection, dynamic mode/tools, queued mutations, review transport/recovery, promotion, compact receipts, and GrillMe retirement verified.");
} finally {
  await rm(root, { recursive: true, force: true });
}

function createDomain() {
  return {
    async context(path) {
      const state = await readJson(path);
      return { action: "context", summary: `orientation r${state.revision}`, revision: state.revision, orientation: { id: state.id, title: state.title } };
    },

    async update(path, params) {
      const state = await readJson(path);
      await delay(15);
      state.counter = (state.counter ?? 0) + (params.changes?.counterDelta ?? 0);
      if (params.understanding) state.currentUnderstanding = { label: "Initial understanding", body: params.understanding };
      state.revision++;
      await writeJson(path, state);
      return { action: "update", summary: `updated r${state.revision}`, revision: state.revision, changed: { updated: ["counter"] } };
    },

    async review(path, params) {
      const state = await readJson(path);
      if (!state.currentUnderstanding) throw new Error("Review requires understanding");
      if (state.reviews.length && !params.replace) throw new Error("Active review exists");
      const review = { id: `R-${params.key}`, title: params.key, points: params.points, understanding: state.currentUnderstanding };
      state.reviews = [review];
      state.revision++;
      await writeJson(path, state);
      return { action: "review", summary: `review ${review.id}`, revision: state.revision, changed: { added: [review.id] }, review };
    },

    async resolveReview(path, params) {
      const state = await readJson(path);
      const review = state.reviews.find(({ id }) => id === params.reviewId);
      if (!review) throw new Error("Review not found");
      state.decisions ??= [];
      for (const outcome of params.outcomes ?? []) {
        state.decisions.push({ id: `D-${outcome.pointId}`, proposalId: outcome.proposalId, createdAt: new Date().toISOString() });
      }
      state.currentUnderstanding = { label: "Current understanding", body: params.understanding };
      state.reviews = [];
      state.revision++;
      await writeJson(path, state);
      return { action: "resolve_review", summary: `resolved ${params.reviewId}`, revision: state.revision, changed: { added: state.decisions.map(({ id }) => id), removed: [params.reviewId] } };
    },

    async promote(path, params) {
      const state = await readJson(path);
      if (params.action === "prepare") return { action: "promote.prepare", summary: "promotion prepared", revision: state.revision };
      for (const target of params.targets ?? []) await readFile(join(params.cwd, target.path), "utf8");
      state.promotions ??= [];
      state.promotions.push({ id: `M-${state.promotions.length + 1}`, targets: params.targets });
      state.revision++;
      await writeJson(path, state);
      return { action: "promote.record", summary: "promotion recorded", revision: state.revision, changed: { added: [state.promotions.at(-1).id] } };
    },
  };
}

async function loadState(root, id) { return readJson(snapshotPath(root, id)); }
async function readJson(path) { return JSON.parse(await readFile(path, "utf8")); }
async function writeJson(path, value) { await writeFile(path, `${JSON.stringify(value, null, 2)}\n`, "utf8"); }
function delay(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }
