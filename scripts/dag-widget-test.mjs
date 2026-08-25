import assert from "node:assert/strict";
import { visibleWidth } from "@earendil-works/pi-tui";
import {
  DagWidgetControllerV2,
  registerCanonicalDagRuntime,
  renderDagWidgetV2,
} from "../extensions/dag-workflow/dag-runtime/index.ts";

function stageStates(current, passed) {
  return Array.from({ length: 9 }, (_, index) => ({ stage: `F${index}`, state: index < passed ? "passed" : index === current ? "active" : "pending" }));
}

const nodeBase = {
  repositoryId: "repo-main",
  current: "active",
  correctnessReady: false,
  admissible: false,
  schedulerOrder: null,
  blockerCodes: [],
  blockerIds: [],
  activeLane: false,
  laneAdmissionSequence: null,
  admittedAt: null,
  retryCount: 0,
  findingCount: 0,
  integrationPosition: null,
  candidateGeneration: 1,
  worker: null,
};
const nodes = [
  { ...nodeBase, alias: "N01", workItemId: "item-live", title: "Verify domain changes", stage: "F3", glyph: "*", activeLane: true, laneAdmissionSequence: 1, worker: { workerId: "worker-live", attemptNumber: 1, terminalStatus: null }, stages: stageStates(3, 3) },
  { ...nodeBase, alias: "N02", workItemId: "item-static", title: "Migrate authority model", stage: "F1", glyph: ":", activeLane: true, laneAdmissionSequence: 2, stages: stageStates(1, 1) },
  { ...nodeBase, alias: "N03", workItemId: "item-dependent-a", title: "Dependent A", stage: "F0", glyph: ">", schedulerOrder: 1, stages: stageStates(0, 0) },
  { ...nodeBase, alias: "N04", workItemId: "item-dependent-b", title: "Dependent B", stage: "F0", glyph: ">", schedulerOrder: 2, stages: stageStates(0, 0) },
];
const projection = {
  schemaVersion: 2,
  kind: "DagExecutionProjectionV2",
  projectionVersion: "2",
  planHash: "sha256:plan",
  runId: "run-widget-prototype",
  runRevision: 21,
  runSnapshotHash: "sha256:run",
  workerProjectionHash: "sha256:workers",
  repositoryProjectionHash: "sha256:repositories",
  schedulerDecisionHash: "sha256:scheduler",
  desired: "running",
  current: "active",
  completion: "open",
  staleReadOnly: false,
  nodes,
  precedence: [
    { precedenceId: "edge-live-a", from: "item-live", to: "item-dependent-a", state: "waiting" },
    { precedenceId: "edge-static-a", from: "item-static", to: "item-dependent-a", state: "waiting" },
    { precedenceId: "edge-static-b", from: "item-static", to: "item-dependent-b", state: "waiting" },
  ],
  trainHeads: [],
  summary: { ready: 0, activeLanes: 2, attention: 0, integrationReady: 0, complete: 0, omittedWorkers: 0 },
  projectionHash: "sha256:projection",
};

for (const width of [1, 12, 19, 20, 50, 80, 120]) {
  for (const terminalRows of [4, 12, 24, 60]) {
    const layout = renderDagWidgetV2(projection, width, terminalRows, "2026-08-18T00:00:00.000Z", { animationFrame: 0, freshLiveAliases: ["N01"] });
    const rowBudget = Math.max(4, Math.min(12, Math.floor(terminalRows / 3)));
    assert(layout.lines.length <= rowBudget, `${width}x${terminalRows} stays within its row budget`);
    assert(layout.lines.every((line) => visibleWidth(line) <= width), `${width}x${terminalRows} honors exact visible width`);
    if (width < 20) assert.deepEqual(layout.activityAliases, [], `${width} columns omit both motion cells and animation authority`);
  }
}

for (const width of [50, 80, 120]) {
  const layout = renderDagWidgetV2(projection, width, 24, "2026-08-18T00:00:00.000Z", { animationFrame: 0, freshLiveAliases: ["N01"] });
  assert(layout.lines.some((line) => line.includes("▶ N03")), `${width} columns render right-pointing dependent aliases`);
  assert(layout.lines.every((line) => !line.includes("▶ >")), `${width} columns omit the redundant dependent state prefix`);
  assert.deepEqual(layout.activityAliases, ["N01", "N02"], `${width} columns expose only rendered anchor lanes for animation authority`);
}
const narrow = renderDagWidgetV2(projection, 50, 24, "2026-08-18T00:00:00.000Z");
assert(narrow.lines.some((line) => line.includes("F3 3/9")), "narrow progress uses current stage plus passed/9");
const wide = renderDagWidgetV2(projection, 80, 24, "2026-08-18T00:00:00.000Z");
assert(wide.lines.some((line) => line.includes("F3 [■■■▶·····]")), "medium/wide progress keeps all nine stage cells");

const movingA = renderDagWidgetV2(projection, 80, 24, "2026-08-18T00:00:00.000Z", { animationFrame: 0, freshLiveAliases: ["N01"] });
const movingB = renderDagWidgetV2(projection, 80, 24, "2026-08-18T00:00:00.000Z", { animationFrame: 1, freshLiveAliases: ["N01"] });
assert.equal(movingA.lines.find((line) => line.includes("*N01")), movingB.lines.find((line) => line.includes("*N01")), "active lane remains static across obsolete animation frames");
assert.equal(movingA.lines.find((line) => line.includes(":N02")), movingB.lines.find((line) => line.includes(":N02")), "non-worker lane remains static");
const frozen = renderDagWidgetV2(projection, 80, 24, "2026-08-18T00:00:00.000Z", { animationFrame: 4, freshLiveAliases: [], diagnostic: "STALE READ-ONLY | source r20" });
assert(frozen.lines.find((line) => line.includes("*N01"))?.startsWith("· "), "stale live disposition freezes instead of claiming motion");
assert(frozen.lines.at(-1).includes("STALE READ-ONLY | source r20"), "stale last-good projection keeps its bounded source diagnostic visible");

const regressedNodes = nodes.map((node) => node.alias === "N01" ? { ...node, stage: "F2", stages: stageStates(2, 2) } : node);
const regressed = renderDagWidgetV2({ ...projection, nodes: regressedNodes }, 80, 24, "2026-08-18T00:00:00.000Z");
assert(regressed.lines.some((line) => line.includes("F2 [■■▶······]")), "rework can regress canonical stage progress without special failure animation");

const denseDependents = Array.from({ length: 20 }, (_, index) => ({ ...nodes[2], alias: `N${String(index + 3).padStart(2, "0")}`, workItemId: `dense-${index}`, title: `Dense ${index}` }));
const denseProjection = { ...projection, nodes: [nodes[0], ...denseDependents], precedence: denseDependents.map((node, index) => ({ precedenceId: `dense-edge-${index}`, from: nodes[0].workItemId, to: node.workItemId, state: "waiting" })), summary: { ...projection.summary, activeLanes: 1 } };
const dense = renderDagWidgetV2(denseProjection, 50, 24, "2026-08-18T00:00:00.000Z");
assert(dense.omittedEdges > 0 && dense.lines.at(-1).includes("edges omitted"), "dense fan-out reports deterministic edge omission");

class FakeIntervals {
  nextId = 1;
  entries = new Map();
  schedule = (callback, milliseconds) => {
    const handle = { id: this.nextId++, unref() {} };
    this.entries.set(handle, { callback, milliseconds });
    return handle;
  };
  clear = (handle) => { this.entries.delete(handle); };
  fire(milliseconds) {
    for (const { callback, milliseconds: interval } of [...this.entries.values()]) if (interval === milliseconds) callback();
  }
  count(milliseconds) { return [...this.entries.values()].filter((entry) => entry.milliseconds === milliseconds).length; }
}

let releaseFirst;
const firstRead = new Promise((resolve) => { releaseFirst = resolve; });
let readCalls = 0;
let inFlight = 0;
let maxInFlight = 0;
let renderRequests = 0;
const intervals = new FakeIntervals();
const controller = new DagWidgetControllerV2({
  async read() {
    readCalls += 1;
    inFlight += 1;
    maxInFlight = Math.max(maxInFlight, inFlight);
    try {
      if (readCalls === 1) await firstRead;
      return { kind: "projection", projection, fresh: true, diagnostic: null };
    } finally { inFlight -= 1; }
  },
  requestRender() { renderRequests += 1; },
  scheduleInterval: intervals.schedule,
  clearScheduledInterval: intervals.clear,
});
const refreshA = controller.refresh();
const refreshB = controller.refresh();
await Promise.resolve();
assert.equal(readCalls, 1, "overlapping refresh requests share one in-flight status read");
releaseFirst();
await Promise.all([refreshA, refreshB]);
assert.equal(readCalls, 2, "an overlapping refresh coalesces into one successor read");
assert.equal(maxInFlight, 1, "status reads are serialized");
controller.noteSelectedAliases(["N01", "N02"]);
assert.deepEqual(controller.snapshot().freshLiveAliases, [], "controller grants no animation authority");
assert.equal(renderRequests, 1, "coalesced identical projections request one initial render");
await controller.refresh();
assert.equal(renderRequests, 1, "unchanged projection hash and diagnostic request no additional render");
assert.equal(intervals.entries.size, 0, "controller creates no animation interval");
controller.dispose();

const slowReleases = [];
let slowReads = 0;
const slowController = new DagWidgetControllerV2({
  async read() {
    slowReads += 1;
    await new Promise((resolve) => { slowReleases.push(resolve); });
    return { kind: "projection", projection, fresh: true, diagnostic: null };
  },
  requestRender() {},
});
const slowA = slowController.refresh();
await Promise.resolve();
const slowB = slowController.refresh();
slowReleases.shift()();
await slowA;
assert.equal(slowReads, 2, "the active refresh caller settles after one read even when a successor is queued");
const slowC = slowController.refresh();
slowReleases.shift()();
await slowB;
assert.equal(slowReads, 3, "each queued refresh batch settles independently under continuous slow-read pressure");
slowReleases.shift()();
await slowC;
slowController.dispose();

let releasePromotedFirst;
let releasePromotedSecond;
let promotedReads = 0;
const promotedController = new DagWidgetControllerV2({
  async read() {
    promotedReads += 1;
    await new Promise((resolve) => {
      if (promotedReads === 1) releasePromotedFirst = resolve;
      else releasePromotedSecond = resolve;
    });
    return { kind: "projection", projection, fresh: true, diagnostic: null };
  },
  requestRender() {},
});
const promotedA = promotedController.refresh();
await Promise.resolve();
const promotedB = promotedController.refresh();
releasePromotedFirst();
await promotedA;
assert.equal(promotedReads, 2, "queued refresh is promoted to one successor read");
promotedController.dispose();
await promotedB;
releasePromotedSecond();

let throwingRenderCalls = 0;
const throwingController = new DagWidgetControllerV2({
  async read() { return { kind: "projection", projection, fresh: true, diagnostic: null }; },
  requestRender() { throwingRenderCalls += 1; throw new Error("UI gone"); },
});
await Promise.all([throwingController.refresh(), throwingController.refresh()]);
assert.equal(throwingRenderCalls, 2, "throwing render requests are consumed without rejecting or stranding either refresh batch");
assert.match(throwingController.snapshot().diagnostic, /render request failed: UI gone/);
throwingController.dispose();

const staleIntervals = new FakeIntervals();
let staleReadCount = 0;
const staleController = new DagWidgetControllerV2({
  async read() { staleReadCount += 1; return { kind: "projection", projection, fresh: staleReadCount === 1, diagnostic: staleReadCount === 1 ? null : "STALE READ-ONLY" }; },
  requestRender() {},
  now: () => 0,
  scheduleInterval: staleIntervals.schedule,
  clearScheduledInterval: staleIntervals.clear,
});
await staleController.refresh();
staleController.noteSelectedAliases(["N01"]);
assert.deepEqual(staleController.snapshot().freshLiveAliases, []);
await staleController.refresh();
assert.equal(staleController.snapshot().diagnostic, "STALE READ-ONLY", "changed diagnostic updates the static projection");
assert.equal(staleIntervals.entries.size, 0, "stale projection creates no animation interval");
staleController.dispose();

let releaseLate;
const lateRead = new Promise((resolve) => { releaseLate = resolve; });
let lateRenderRequests = 0;
const lateController = new DagWidgetControllerV2({
  async read() { await lateRead; return { kind: "projection", projection, fresh: true, diagnostic: null }; },
  requestRender() { lateRenderRequests += 1; },
});
const lateRefresh = lateController.refresh();
await Promise.resolve();
lateController.dispose();
releaseLate();
await lateRefresh;
assert.equal(lateRenderRequests, 0, "a refresh completing after disposal cannot publish or request a render");
assert.equal(lateController.snapshot().projection, null, "disposed controller cannot restore a cleared projection");

let releaseTerminal;
const terminalRead = new Promise((resolve) => { releaseTerminal = resolve; });
let terminalReads = 0;
let terminalCallbacks = 0;
const terminalController = new DagWidgetControllerV2({
  async read() { terminalReads += 1; await terminalRead; return { kind: "terminal" }; },
  requestRender() {},
  onTerminal() { terminalCallbacks += 1; },
});
const terminalRefreshA = terminalController.refresh();
const terminalRefreshB = terminalController.refresh();
releaseTerminal();
await Promise.all([terminalRefreshA, terminalRefreshB]);
await terminalController.refresh();
assert.equal(terminalReads, 1, "terminal observation disposes the controller without running a queued successor refresh");
assert.equal(terminalCallbacks, 1, "terminal callback runs exactly once");

const pi = { handlers: new Map(), tools: [], on(event, handler) { this.handlers.set(event, [...(this.handlers.get(event) ?? []), handler]); }, registerTool(tool) { this.tools.push(tool); } };
const widgetCalls = [];
let detached = false;
registerCanonicalDagRuntime(pi, {
  async binding() { return { runId: projection.runId }; },
  async resumeBound() {},
  async status() { return { projection, stale: null, state: { completion: { state: "open" }, current: { run: "active" } } }; },
  async detach() { detached = true; },
});
const context = { hasUI: true, mode: "tui", cwd: "/tmp", sessionManager: { getSessionId: () => "widget-test" }, ui: { setWidget(id, value) { widgetCalls.push({ id, value }); } } };
for (const handler of pi.handlers.get("session_start") ?? []) await handler({}, context);
const widgetFactory = widgetCalls.findLast(({ value }) => typeof value === "function")?.value;
assert.equal(typeof widgetFactory, "function", "interactive TUI session installs the V2 widget component");
const component = widgetFactory({ terminal: { rows: 24 }, requestRender() {} });
assert.deepEqual(component.render(0), [], "zero-width render returns no rows instead of widening Pi's contract");
for (const width of [1, 12, 19, 50, 80]) assert(component.render(width).every((line) => visibleWidth(line) <= width), `integration passes Pi's exact ${width}-column render contract without widening`);
for (const handler of pi.handlers.get("session_shutdown") ?? []) await handler();
assert.equal(widgetCalls.at(-1).value, undefined, "session shutdown clears the installed widget");
assert.equal(detached, true, "session shutdown detaches the conductor after disposing widget state");

const overlapPi = { handlers: new Map(), tools: [], on(event, handler) { this.handlers.set(event, [...(this.handlers.get(event) ?? []), handler]); }, registerTool(tool) { this.tools.push(tool); } };
let releaseOldResume;
const oldResume = new Promise((resolve) => { releaseOldResume = resolve; });
let resumeCalls = 0;
registerCanonicalDagRuntime(overlapPi, {
  async binding() { return null; },
  async resumeBound() { resumeCalls += 1; if (resumeCalls === 1) await oldResume; },
  async detach() {},
});
const firstSessionCalls = [];
const secondSessionCalls = [];
const firstContext = { hasUI: true, mode: "tui", cwd: "/tmp", sessionManager: { getSessionId: () => "first" }, ui: { setWidget(id, value) { firstSessionCalls.push({ id, value }); } } };
const secondContext = { hasUI: true, mode: "tui", cwd: "/tmp", sessionManager: { getSessionId: () => "second" }, ui: { setWidget(id, value) { secondSessionCalls.push({ id, value }); } } };
const startHandler = overlapPi.handlers.get("session_start")[0];
const firstStart = startHandler({}, firstContext);
await Promise.resolve();
await startHandler({}, secondContext);
releaseOldResume();
await firstStart;
assert.equal(firstSessionCalls.some(({ value }) => typeof value === "function"), false, "a superseded slow session start cannot install its stale widget generation");
assert.equal(secondSessionCalls.filter(({ value }) => typeof value === "function").length, 1, "the current session alone installs a widget after overlapping startup");
for (const handler of overlapPi.handlers.get("session_shutdown") ?? []) await handler();

console.log("DAG widget V2 tests OK: responsive graph layout, static activity, deduplicated refresh, and disposal fencing pass");
