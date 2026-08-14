import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, mkdir, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import {
  DagPlanningSelectorError,
  DagPlanningStoreBusyError,
  DagPlanningStoreConflictError,
  DagPlanningStoreV1,
  createDagPlanningPlanV1,
  dagPlanningPlanHashV1,
  parseDagPlanningPlanV1,
  projectDagPlanningGraphV1,
  projectDagPlanningLineageV1,
  projectDagPlanningNodeV1,
  renderDagPlanningGraphV1,
  renderDagPlanningMarkdownV1,
  sealDagPlanningPlanV1,
  selectDagPlanningPlanV1,
  selectDagPlanningWorkItemV1,
  validateDagPlanningPlanV1,
} from "../extensions/dag-workflow/planning/index.ts";

const NOW = "2026-08-13T00:00:00.000Z";
const tests = [];
const test = (name, fn) => tests.push([name, fn]);

function planInput(planId = "plan-alpha") {
  return {
    planId,
    status: "draft",
    title: "Thin planning vertical slice",
    focusId: "focus-thin-planning",
    repository: {
      repositoryId: "repo-main",
      baselineCommit: "1".repeat(40),
      baselineTree: "2".repeat(40),
      targetBranch: "main",
    },
    source: {
      refs: [
        {
          kind: "project_model_object",
          collection: "decisions",
          objectId: "DEC-plan-workflow",
          semanticHash: `sha256:${"3".repeat(64)}`,
          summary: "Ship one focus-linked plan workflow.",
        },
        {
          kind: "generated_spec",
          path: "spec/model-aware-dag-runtime/spec.md",
          contentHash: `sha256:${"4".repeat(64)}`,
          summary: "Current accepted planning direction.",
        },
        { kind: "external", ref: "issue:123" },
      ],
      scopeSummary: "Add thin planning persistence and projections without command wiring.",
    },
    architecture: {
      outcomes: [
        { id: "out-plan", description: "Persist one inspectable revisioned plan." },
        { id: "out-show", description: "Render exact deterministic plan views." },
      ],
      nonGoals: ["Do not wire commands.", "Do not add phase facts or receipt chains."],
      notes: ["Keep planning authority in one JSON record."],
      risks: ["Concurrent writers could otherwise lose revisions."],
    },
    workItems: [
      {
        id: "store",
        title: "Build the plan store",
        objective: "Persist strictly validated draft revisions durably.",
        outcomeIds: ["out-plan"],
        context: ["Use repository-local ignored storage."],
        checks: ["Round-trip strict JSON.", "Reject stale expected revisions."],
        dependsOn: [],
        risk: "high",
        riskNotes: ["Filesystem mutation must be process-shared."],
        constraints: ["Do not use an event log."],
      },
      {
        id: "views",
        title: "Build plan projections",
        objective: "Project Markdown, a static graph, and an exact node packet.",
        outcomeIds: ["out-show"],
        context: ["The JSON plan remains authoritative."],
        checks: ["Render twice with byte-identical output."],
        dependsOn: ["store"],
        risk: "low",
        riskNotes: [],
      },
    ],
    constraints: {
      maxConcurrency: 2,
      mutexGroups: [{ id: "plan-file", workItemIds: ["store", "views"], reason: "Both define the public planning contract." }],
    },
    integration: {
      strategy: "dependency_order",
      checks: ["node scripts/dag-planning-test.mjs"],
      finalChecks: ["Plan record reloads and all projections match."],
      prefixCommands: [{ id: "prefix-git-check", argv: ["git", "diff-tree", "--check", "--root", "HEAD"] }],
      finalCommands: [{ id: "final-git-check", argv: ["git", "diff-tree", "--check", "--root", "HEAD"] }],
    },
    approval: { status: "pending", by: null, at: null, note: null },
    authorization: { status: "not_authorized", by: null, at: null, scope: [], maxConcurrency: null, note: null },
  };
}

function rehash(plan) {
  plan.planHash = dagPlanningPlanHashV1(plan);
  return plan;
}

function invalidPlan(plan, mutate) {
  const copy = structuredClone(plan);
  mutate(copy);
  return rehash(copy);
}

function resealRevision(plan, revision, mutate = () => {}) {
  const copy = structuredClone(plan);
  mutate(copy);
  delete copy.planHash;
  copy.revision = revision;
  copy.updatedAt = `2026-08-13T00:0${revision}:00.000Z`;
  return sealDagPlanningPlanV1(copy);
}

async function expectReject(fn, pattern, ErrorClass) {
  let caught;
  try { await fn(); } catch (error) { caught = error; }
  assert(caught, "expected operation to reject");
  if (ErrorClass) assert(caught instanceof ErrorClass, `expected ${ErrorClass.name}, received ${caught?.constructor?.name}`);
  if (pattern) assert.match(String(caught.message), pattern);
  return caught;
}

async function waitForFile(path) {
  for (let attempt = 0; attempt < 500; attempt += 1) {
    try { await readFile(path, "utf8"); return; } catch {}
    await delay(10);
  }
  throw new Error(`Timed out waiting for ${path}`);
}

async function runChild(root, planId, marker) {
  const store = new DagPlanningStoreV1(root);
  const current = await store.read(planId);
  await store.mutateDraft(planId, current.revision, async (draft) => {
    await writeFile(marker, "locked\n", "utf8");
    await delay(300);
    draft.architecture.notes.push("Child process held the shared lock.");
  }, "2026-08-13T00:01:00.000Z");
}

if (process.argv[2] === "__lock_child") {
  await runChild(process.argv[3], process.argv[4], process.argv[5]);
  process.exit(0);
}

test("artifact binds exact typed sources and hashes only static plan semantics", () => {
  const plan = createDagPlanningPlanV1(planInput(), NOW);
  assert.equal(plan.revision, 1);
  assert.equal(plan.focusId, "focus-thin-planning");
  assert.match(plan.planHash, /^sha256:[0-9a-f]{64}$/);
  assert.equal(plan.planHash, dagPlanningPlanHashV1(plan));
  assert.equal(validateDagPlanningPlanV1(plan).ok, true);

  const decisionOnly = structuredClone(plan);
  decisionOnly.revision = 99;
  decisionOnly.status = "ready";
  decisionOnly.updatedAt = "2026-08-13T03:00:00.000Z";
  decisionOnly.approval = { status: "approved", by: "reviewer", at: "2026-08-13T02:00:00.000Z", note: null };
  assert.equal(dagPlanningPlanHashV1(decisionOnly), plan.planHash, "revision, status, timestamps, and decisions do not change static identity");

  const unknown = structuredClone(plan);
  unknown.manifest = {};
  assert.equal(validateDagPlanningPlanV1(unknown).ok, false, "unknown fields fail the closed schema");
  const changed = structuredClone(plan);
  changed.title = "Changed without resealing";
  assert(validateDagPlanningPlanV1(changed).issues.some(({ path }) => path === "/planHash"), "static content drift fails validation");
  const looseRef = structuredClone(plan);
  looseRef.source.refs[0].extra = "not exact";
  assert.equal(validateDagPlanningPlanV1(looseRef).ok, false, "typed source bindings are structurally exact");
  const badSpecPath = invalidPlan(plan, (copy) => copy.source.refs[1].path = "../outside.md");
  assert(validateDagPlanningPlanV1(badSpecPath).issues.some(({ path }) => path.endsWith("/path")), "generated spec bindings are repository-relative");

  const missing = invalidPlan(plan, (copy) => copy.workItems[1].dependsOn = ["missing"]);
  assert(validateDagPlanningPlanV1(missing).issues.some(({ message }) => message.includes("missing work item")), "missing dependency references fail");
  const cycle = invalidPlan(plan, (copy) => copy.workItems[0].dependsOn = ["views"]);
  assert(validateDagPlanningPlanV1(cycle).issues.some(({ message }) => message.includes("dependency cycle")), "dependency cycles fail");
  const badOutcome = invalidPlan(plan, (copy) => copy.workItems[0].outcomeIds = ["unknown"]);
  assert(validateDagPlanningPlanV1(badOutcome).issues.some(({ message }) => message.includes("missing outcome")), "missing outcome references fail");
  const inconsistentReady = invalidPlan(plan, (copy) => copy.status = "ready");
  assert(validateDagPlanningPlanV1(inconsistentReady).issues.some(({ path }) => path === "/approval/status"), "ready plans require ordinary approval fields");
  const partialAuthorization = invalidPlan(plan, (copy) => {
    copy.status = "ready";
    copy.approval = { status: "approved", by: "user", at: NOW, note: null };
    copy.authorization = { status: "authorized", by: "user", at: NOW, scope: ["store"], maxConcurrency: 1, note: null };
  });
  assert(validateDagPlanningPlanV1(partialAuthorization).issues.some(({ path }) => path === "/authorization/scope"), "V1 never expands a partial authorization into whole-plan runtime authority");

  const nullFocus = createDagPlanningPlanV1({ ...planInput("unfocused"), focusId: null }, NOW);
  assert.equal(nullFocus.focusId, null, "an explicitly unbound plan uses null rather than an omitted or inferred focus");
  const text = JSON.stringify(plan);
  const duplicate = text.replace('"planId":"plan-alpha"', '"planId":"plan-alpha","planId":"plan-alpha"');
  assert.throws(() => parseDagPlanningPlanV1(duplicate), /duplicate object key/, "strict JSON rejects duplicate keys");
});

test("projections are deterministic and node selectors are exact", () => {
  const plan = createDagPlanningPlanV1(planInput(), NOW);
  const markdown = renderDagPlanningMarkdownV1(plan);
  assert.equal(markdown, renderDagPlanningMarkdownV1(structuredClone(plan)));
  assert.match(markdown, /Static plan hash/);
  assert.match(markdown, /exact record `plan-alpha@1`/);
  assert.match(markdown, /project model `decisions:DEC-plan-workflow`/);
  assert.match(markdown, /generated spec `spec\/model-aware-dag-runtime\/spec.md`/);
  assert.match(markdown, /external `issue:123`/);
  assert.match(markdown, /### N02 · Build plan projections/);
  const graph = projectDagPlanningGraphV1(plan);
  assert.equal(graph.recordSelector, "plan-alpha@1");
  assert.deepEqual(graph.edges, [{ from: "store", to: "views", fromAlias: "N01", toAlias: "N02" }]);
  const graphText = renderDagPlanningGraphV1(plan);
  assert.equal(graphText, renderDagPlanningGraphV1(plan));
  assert.match(graphText, /N01 -> N02/);

  const nodeById = projectDagPlanningNodeV1(plan, "views");
  const nodeByAlias = projectDagPlanningNodeV1(plan, "N02");
  assert.deepEqual(nodeByAlias, nodeById);
  assert.equal(nodeById.recordSelector, "plan-alpha@1");
  assert.deepEqual(nodeById.outcomes.map(({ id }) => id), ["out-show"]);
  assert.deepEqual(nodeById.dependencies.map(({ workItemId }) => workItemId), ["store"]);
  assert.throws(() => projectDagPlanningNodeV1(plan, "view"), DagPlanningSelectorError, "prefixes never select a node");

  const ambiguousInput = planInput("alias-collision");
  ambiguousInput.workItems[1].id = "N01";
  ambiguousInput.constraints.mutexGroups[0].workItemIds[1] = "N01";
  const ambiguous = createDagPlanningPlanV1(ambiguousInput, NOW);
  assert.throws(() => selectDagPlanningWorkItemV1(ambiguous, "N01"), /ambiguous/, "an exact ID/alias collision fails rather than guessing");
});

test("plan selectors distinguish exact heads and historical revisions", () => {
  const first = createDagPlanningPlanV1(planInput("first"), NOW);
  const firstRevisionTwo = resealRevision(first, 2, (copy) => copy.title = "First revised");
  const second = createDagPlanningPlanV1(planInput("second"), NOW);
  assert.equal(selectDagPlanningPlanV1([second], undefined).planId, "second");
  assert.equal(selectDagPlanningPlanV1([firstRevisionTwo, first], "first").revision, 2, "an exact plan ID selects its head");
  assert.equal(selectDagPlanningPlanV1([firstRevisionTwo, first], "first@1").revision, 1, "planId@revision selects retained history");
  assert.throws(() => selectDagPlanningPlanV1([first, second], undefined), /selector is required/);
  assert.throws(() => selectDagPlanningPlanV1([first, second], "fir"), /exactly matches/);
  let unsupportedHash;
  try { selectDagPlanningPlanV1([first], first.planHash); } catch (error) { unsupportedHash = error; }
  assert(unsupportedHash instanceof DagPlanningSelectorError);
  assert.match(unsupportedHash.message, /planId@revision/);
  assert(unsupportedHash.candidates.every((candidate) => candidate === "first" || /^first@[1-9][0-9]*$/.test(candidate)), "advertised candidates are accepted selector forms");
  assert.throws(() => selectDagPlanningPlanV1([first, structuredClone(first)], "first@1"), /ambiguous/);

  const lineage = projectDagPlanningLineageV1([firstRevisionTwo, first]);
  assert.equal(lineage.headSelector, "first");
  assert.equal(lineage.headRevision, 2);
  assert.deepEqual(lineage.revisions.map(({ selector, isHead }) => [selector, isHead]), [["first@1", false], ["first@2", true]]);
});

test("store retains immutable history and records approval then independent authorization", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-planning-"));
  try {
    const store = new DagPlanningStoreV1(root);
    const initial = createDagPlanningPlanV1(planInput(), NOW);
    await store.create(initial);
    assert.deepEqual(await store.list(), [{ planId: initial.planId, revision: 1, status: "draft", title: initial.title, planHash: initial.planHash }]);
    const storedText = await readFile(store.pathFor(initial.planId), "utf8");
    const revisionOneText = await readFile(store.revisionPathFor(initial.planId, 1), "utf8");
    const parsedStored = parseDagPlanningPlanV1(storedText);
    assert.equal(JSON.stringify(parsedStored), JSON.stringify(JSON.parse(storedText)), "stored record round-trips through strict JSON");
    assert.equal(parsedStored.planHash, initial.planHash);
    assert(storedText.includes("\n  \"approval\""), "stored JSON is inspectable pretty-printed content");

    const marker = join(root, "child-locked");
    const child = spawn(process.execPath, [process.argv[1], "__lock_child", root, initial.planId, marker], { stdio: ["ignore", "pipe", "pipe"] });
    let childError = "";
    child.stderr.on("data", (chunk) => childError += chunk);
    await waitForFile(marker);
    const ownerPath = join(store.locksDirectory, `${initial.planId}.lock`, "owner.json");
    const liveOwner = JSON.parse(await readFile(ownerPath, "utf8"));
    await writeFile(ownerPath, `${JSON.stringify({ ...liveOwner, acquiredAt: "2000-01-01T00:00:00.000Z" })}\n`, "utf8");
    await expectReject(
      () => store.mutateDraft(initial.planId, 1, (draft) => { draft.title = "Lost update"; }),
      /locked by another process/,
      DagPlanningStoreBusyError,
    );
    const exitCode = await new Promise((resolve) => child.on("exit", resolve));
    assert.equal(exitCode, 0, childError);

    const revisionTwo = await store.read(initial.planId);
    assert.equal(revisionTwo.revision, 2);
    assert.notEqual(revisionTwo.planHash, initial.planHash);
    assert.equal(await readFile(store.revisionPathFor(initial.planId, 1), "utf8"), revisionOneText, "revision one remains byte-identical");
    await expectReject(() => store.mutateDraft(initial.planId, 1, () => {}), /revision conflict/, DagPlanningStoreConflictError);
    await expectReject(() => store.mutateDraft(initial.planId, 2, (draft) => { draft.status = "ready"; }), /use decision mutation/);

    const approved = await store.mutateDecision(initial.planId, 2, (decision) => {
      decision.status = "ready";
      decision.approval = { status: "approved", by: "user", at: "2026-08-13T00:02:00.000Z", note: "Reviewed." };
    }, "2026-08-13T00:02:00.000Z");
    assert.equal(approved.plan.revision, 3);
    assert.equal(approved.plan.status, "ready");
    assert.equal(approved.plan.planHash, revisionTwo.planHash, "approval leaves static plan identity unchanged");

    const authorized = await store.mutateDecision(initial.planId, 3, (decision) => {
      decision.authorization = {
        status: "authorized",
        by: "operator",
        at: "2026-08-13T00:03:00.000Z",
        scope: ["store", "views"],
        maxConcurrency: 1,
        note: "Proceed.",
      };
    }, "2026-08-13T00:03:00.000Z");
    assert.equal(authorized.plan.revision, 4);
    assert.equal(authorized.plan.authorization.status, "authorized");
    assert.equal(authorized.plan.planHash, approved.plan.planHash, "authorization leaves static plan identity unchanged");
    await expectReject(() => store.mutateDraft(initial.planId, 4, () => {}), /only drafts may be mutated/);

    const history = await store.listRevisions(initial.planId);
    assert.deepEqual(history.map(({ revision }) => revision), [1, 2, 3, 4]);
    assert.equal((await store.read(initial.planId, 1)).planHash, initial.planHash);
    assert.equal((await store.select("plan-alpha")).revision, 4, "plan ID selects the head");
    assert.equal((await store.select("plan-alpha@2")).revision, 2, "planId@revision selects exact retained history");
    assert.deepEqual(projectDagPlanningLineageV1(history).revisions.map(({ selector }) => selector), ["plan-alpha@1", "plan-alpha@2", "plan-alpha@3", "plan-alpha@4"]);
    const leftovers = (await readdir(store.directory)).filter((name) => name.endsWith(".tmp") || name.endsWith(".pending"));
    assert.deepEqual(leftovers, [], "durable replacement leaves no temporary snapshots");

    const second = createDagPlanningPlanV1(planInput("plan-beta"), NOW);
    const deadLock = join(store.locksDirectory, "plan-beta.lock");
    await mkdir(deadLock);
    await writeFile(join(deadLock, "owner.json"), `${JSON.stringify({ pid: 999_999_999, processStartId: "linux-proc:1", acquiredAt: "2000-01-01T00:00:00.000Z" })}\n`);
    await store.create(second);
    assert.equal((await store.read("plan-beta")).revision, 1, "a definitely dead owner is recovered");

    const third = createDagPlanningPlanV1(planInput("plan-gamma"), NOW);
    const reusedPidLock = join(store.locksDirectory, "plan-gamma.lock");
    await mkdir(reusedPidLock);
    await writeFile(join(reusedPidLock, "owner.json"), `${JSON.stringify({ pid: process.pid, processStartId: "linux-proc:0", acquiredAt: "2000-01-01T00:00:00.000Z" })}\n`);
    await store.create(third);
    assert.equal((await store.read("plan-gamma")).revision, 1, "a PID with a mismatched start identity is recovered");

    const orphaned = createDagPlanningPlanV1(planInput("plan-orphaned"), NOW);
    const orphanPath = store.revisionPathFor(orphaned.planId, 1);
    await mkdir(dirname(orphanPath), { recursive: true });
    await writeFile(orphanPath, "{\"incomplete\":true}\n");
    await store.create(orphaned);
    assert.equal((await store.read(orphaned.planId)).planHash, orphaned.planHash, "retry removes history published before an uncommitted head crash");

    await expectReject(() => store.select(), /selector is required/, DagPlanningSelectorError);
    await expectReject(() => store.select(second.planHash), /planId@revision/, DagPlanningSelectorError);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("store refuses repository paths through symlinks", async () => {
  const root = await mkdtemp(join(tmpdir(), "dag-planning-link-"));
  const outside = await mkdtemp(join(tmpdir(), "dag-planning-outside-"));
  try {
    await mkdir(join(outside, "plans"));
    await symlink(outside, join(root, "linked"));
    const store = new DagPlanningStoreV1(root, "linked/plans");
    await expectReject(() => store.list(), /symlink/);
  } finally {
    await rm(root, { recursive: true, force: true });
    await rm(outside, { recursive: true, force: true });
  }
});

let failures = 0;
for (const [name, fn] of tests) {
  try {
    await fn();
    console.log(`ok - ${name}`);
  } catch (error) {
    failures += 1;
    console.error(`not ok - ${name}`);
    console.error(error?.stack ?? error);
  }
}
if (failures > 0) process.exitCode = 1;
else console.log(`ok - ${tests.length} planning tests passed`);
