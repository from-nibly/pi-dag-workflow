import assert from "node:assert/strict";
import { access, mkdtemp, readFile, rm, writeFile, mkdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { BrainstormPrototype } from "./brainstorm-tools.mjs";

const root = await mkdtemp(join(tmpdir(), "brainstorm-object-prototype-"));
const runtime = new BrainstormPrototype(root);
const id = "operant-auth-review";

try {
  let result = await runtime.start({ id, title: "Operant authorization and changeset review" });
  assert.equal(result.revision, 0);
  assert.equal(result.resumed, false);
  assert.equal((await runtime.start({ id, title: "ignored" })).resumed, true);

  result = await runtime.execute("dag_brainstorm_update", {
    id,
    add: {
      neighborhoods: [{ key: "auth", title: "Authorization and changesets", status: "active" }],
      tangents: [
        { key: "auth-delivery", title: "Authorization delivery", source: "user", neighborhoods: ["auth"] },
        {
          key: "recovery-risk",
          title: "Recovery risk",
          neighborhoods: ["auth"],
          whyNow: "Delivery research exposed restart uncertainty.",
          researchNeeded: "Inspect restart and credential persistence behavior.",
          connection: "Adjacent to authorization delivery.",
        },
      ],
      evidence: [{ key: "session", kind: "session", source: "Operant review", summary: "Requester and approver may be remote." }],
      questions: [
        { key: "token", tangents: ["auth-delivery"], prompt: "Who receives an approved credential?", evidence: ["session"] },
        { key: "preview", tangents: ["auth-delivery"], prompt: "Must commit apply only previewed effects?" },
      ],
    },
    signals: [{ tangent: "auth-delivery", type: "foundation", rationale: "Approval and recovery depend on delivery." }],
  });
  assert.equal(result.action, "update");
  assert.match(result.next.join("\n"), /dag_brainstorm_review/);
  assert.ok(result.changed.added.includes("Q-token"));

  let context = await runtime.execute("dag_brainstorm_context", { id });
  assert.equal(context.data.openQuestionCount, 2);
  assert.equal(context.data.frontier.find(({ id: tangentId }) => tangentId === "T-recovery-risk").status, "proposed");
  await assert.rejects(runtime.execute("dag_brainstorm_context", { id, view: "state" }), /Unknown context view/);

  result = await runtime.execute("dag_brainstorm_review", {
    id,
    key: "auth-review",
    title: "Authorization sharp corners",
    understanding: {
      body: "Initial repository evidence indicates that requester-owned redemption best preserves delivery boundaries, while preview integrity remains unsettled.",
    },
    points: [
      {
        key: "credential-delivery",
        title: "Credential delivery",
        question: "token",
        context: "The requester and approver may be on different machines, so delivery ownership changes the recovery model.",
        options: [
          {
            key: "requester-redemption",
            title: "Requester redemption",
            description: "The requester redeems and installs after approval.",
            contract: "The requester redeems and installs approved credentials.",
            rationale: "The requester owns the target environment and may be remote from the approver.",
            recommended: true,
          },
          {
            key: "approver-delivery",
            title: "Approver delivery",
            description: "The approver receives a transferable credential.",
            contract: "The approver receives and transfers approved credentials.",
          },
        ],
      },
      {
        key: "preview-integrity",
        title: "Preview integrity",
        question: "preview",
        context: "Preview and commit behavior must remain understandable, but this point is intentionally ignored in sparse feedback.",
      },
    ],
  });
  assert.equal(result.action, "review");
  assert.equal(result.data.points.length, 2);
  assert.equal(result.data.points[0].options[0].recommended, true);
  assert.equal("context" in result.data.points[0], false, "review mutation receipt does not echo authored prose");
  assert.match(result.next.join("\n"), /optional Lavish/);
  const reviewId = result.data.id;
  const deliveryPoint = result.data.points[0];
  const selectedProposal = deliveryPoint.options.find(({ recommended }) => recommended).id;
  const reviewContext = await runtime.execute("dag_brainstorm_context", { id, view: "review", reviewId });
  assert.equal(reviewContext.data.understanding.label, "Initial understanding");
  assert.match(reviewContext.data.understanding.body, /requester-owned redemption/);
  assert.equal(reviewContext.data.points[0].other, true);
  assert.match(reviewContext.data.points[0].context, /different machines/);

  result = await runtime.execute("dag_brainstorm_resolve_review", {
    id,
    reviewId,
    outcomes: [{ pointId: deliveryPoint.id, action: "accept", proposalId: selectedProposal }],
    understanding: {
      body: "Requester-owned redemption keeps installation authority with the environment owner; preview integrity is still unresolved because the user did not judge it.",
    },
  });
  assert.equal(result.action, "resolve_review");
  assert.equal(result.data.resolution.decisionIds.length, 1);
  assert.equal(result.data.resolution.unresolvedPointIds.length, 1);
  assert.equal(result.data.frontier.find(({ id: tangentId }) => tangentId === "T-recovery-risk").status, "tracked");
  const decisionId = result.data.resolution.decisionIds[0];

  const state = await runtime.load(id);
  assert.equal(state.schemaVersion, 2);
  assert.equal("feedback" in state, false, "verbatim responses and unprocessed renderer submissions are not datastore state");
  assert.equal("turns" in state, false, "reviews replace prototype turns");
  assert.equal(state.reviews.length, 0, "resolved review packets are discarded");
  assert.equal(state.currentUnderstanding.label, "Current understanding");
  assert.equal(state.currentUnderstanding.source.reviewId, reviewId);
  assert.deepEqual(state.currentUnderstanding.source.decisionIds, [decisionId]);
  assert.equal(state.questions.find(({ id: questionId }) => questionId === "Q-token").status, "answered");
  assert.equal(state.questions.find(({ id: questionId }) => questionId === "Q-preview").status, "open", "ignored points remain unresolved");

  context = await runtime.execute("dag_brainstorm_context", { id, view: "entities", ids: [decisionId, "Q-preview"] });
  assert.equal(context.data.length, 2);
  assert.ok(context.next.length > 0, "AXI-like results suggest structurally possible next actions");

  result = await runtime.execute("dag_brainstorm_promote", { id, action: "prepare", decisionIds: [decisionId], targetHints: ["spec/auth/spec.md"] });
  assert.equal(result.action, "promote.prepare");
  assert.equal(result.data.decisions[0].id, decisionId);
  await assert.rejects(access(join(root, "spec", "auth", "spec.md")), /ENOENT/, "prepare does not write specs");

  const specPath = join(root, "spec", "auth", "spec.md");
  await mkdir(dirname(specPath), { recursive: true });
  await writeFile(specPath, "# Authorization\n\nThe requester redeems approved credentials.\n", "utf8");
  result = await runtime.execute("dag_brainstorm_promote", {
    id,
    action: "record",
    key: "auth-contract",
    targets: [{ path: "spec/auth/spec.md", decisionIds: [decisionId] }],
  });
  assert.equal(result.action, "promote.record");
  assert.match(await readFile(specPath, "utf8"), /requester redeems/);

  const reloaded = new BrainstormPrototype(root);
  context = await reloaded.execute("dag_brainstorm_context", { id, view: "promotion_ready" });
  assert.equal(context.data.some(({ id: itemId }) => itemId === decisionId), false);

  await new Promise((resolve) => setTimeout(resolve, 2));
  await reloaded.execute("dag_brainstorm_update", {
    id,
    patch: [{ collection: "decisions", id: decisionId, changes: { contract: "The requester securely redeems and installs approved credentials." } }],
  });
  context = await reloaded.execute("dag_brainstorm_context", { id, view: "promotion_ready" });
  assert.equal(context.data.some(({ id: itemId }) => itemId === decisionId), true, "changed promoted decisions become promotion-ready again");
  await writeFile(specPath, "# Authorization\n\nThe requester securely redeems approved credentials.\n", "utf8");
  await reloaded.execute("dag_brainstorm_promote", {
    id,
    action: "record",
    key: "auth-contract-refinement",
    targets: [{ path: "spec/auth/spec.md", decisionIds: [decisionId] }],
  });
  context = await reloaded.execute("dag_brainstorm_context", { id, view: "promotion_ready" });
  assert.equal(context.data.some(({ id: itemId }) => itemId === decisionId), false);
  await assert.rejects(reloaded.execute("dag_brainstorm_record_feedback", { id }), /Unknown brainstorm tool/);

  console.log("Object tools prototype OK: renderer-neutral five-tool flow, mechanical defaults, compact context, direct review resolution, explicit promotion handoff, and reload verified.");
} finally {
  await rm(root, { recursive: true, force: true });
}
