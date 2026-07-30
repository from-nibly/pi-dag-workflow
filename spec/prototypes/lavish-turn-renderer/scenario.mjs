import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { tmpdir } from "node:os";
import { LavishCliAdapter, parsePollOutput } from "./lavish-cli.mjs";
import { LavishTurnLifecycle } from "./lifecycle.mjs";
import { artifactPaths, validateTurnProjection } from "./projection.mjs";
import { renderTurn } from "./renderer.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const fixtures = join(here, "fixtures");
const projection = JSON.parse(await readFile(join(fixtures, "whole-turn.json"), "utf8"));
const html = renderTurn(projection);

assert.deepEqual(validateTurnProjection(projection), [], "fixture satisfies the versioned projection contract");
const malformed = structuredClone(projection);
malformed.review.points[1].options[0].id = malformed.review.points[1].options[1].id;
assert(validateTurnProjection(malformed).some((error) => error.includes("duplicate option")), "projection rejects duplicate option IDs");
assert.match(html, /Current understanding/);
assert.match(html, /Model delta/);
assert.match(html, /Renderer boundary review/);
assert.match(html, /Prototype fixture/);
assert.match(html, /do not resolve the repository project model/, "fixture data cannot be mistaken for active authority");
assert.doesNotMatch(html, /Exact authority payload/, "internal authority serialization is not a human decision surface");
assert.equal((html.match(/class="decision-form"/g) ?? []).length, 2, "one turn supports multiple independent decisions");
assert.equal((html.match(/value="__other__"/g) ?? []).length, 2, "every decision includes an explicit Other radio path");
assert.equal((html.match(/name="responseText"/g) ?? []).length, 2, "each decision has a separate always-available response box");
assert.doesNotMatch(html, /id="send-feedback"/, "the shell does not duplicate Lavish's top send control");
assert.match(html, /window\.lavish\.queuePrompt/);
assert.match(html, /data-option-hash="sha256:option-separate"/, "standard controls carry option freshness hashes");
assert.match(html, /Try the demo counter/, "trusted interactive HTML explains its purpose");
assert.doesNotMatch(html, /cdn\.jsdelivr|https:\/\//, "shell is self-contained and offline");
assert(html.indexOf("Why compare these boundaries?") < html.indexOf('<section id="review"'), "before-review blocks preserve placement");
assert.match(html, /What happens next/, "the frontier explains the post-turn handoff");
assert(html.indexOf("Lifecycle responsibilities") > html.indexOf("Adapter boundary"), "point blocks render inside their point");

const committedSample = await readFile(join(here, "sample-turn.html"), "utf8");
assert.equal(committedSample, html, "committed sample matches deterministic rendering");

const feedbackRaw = await readFile(join(fixtures, "poll-feedback.toon"), "utf8");
const feedback = parsePollOutput(feedbackRaw);
assert.equal(feedback.session.status, "feedback");
assert.equal(feedback.prompts.length, 2);
assert.equal(feedback.prompts[0].tag, "model-review");
assert.match(feedback.prompts[0].prompt, /"pointId":"point-adapter"/);
assert.equal(JSON.stringify(feedback).includes("ignored large snapshot"), false, "DOM snapshot is dropped");
const clipped = parsePollOutput(feedbackRaw, { maxPrompts: 1, maxPromptBytes: 80, maxTotalBytes: 500 });
assert.equal(clipped.prompts.length, 1);
assert(clipped.truncation.truncated && clipped.truncation.droppedPrompts === 1, "feedback caps report prompt loss explicitly");
assert(Buffer.byteLength(clipped.prompts[0].prompt) <= 80, "individual prompt byte cap is enforced");
const layout = parsePollOutput(await readFile(join(fixtures, "poll-layout.toon"), "utf8"));
assert.equal(layout.layoutWarnings.length, 2, "compact TOON table warnings are recognized");
assert.equal(layout.layoutWarnings[0].severity, "error");
assert.equal(layout.layoutWarnings[1].persistent, false);
const ended = parsePollOutput(await readFile(join(fixtures, "poll-ended.toon"), "utf8"));
assert.equal(ended.session.endedBy, "user");

const root = await mkdtemp(join(tmpdir(), "lavish-turn-renderer-"));
try {
  const log = join(root, "fake-cli.log");
  const cli = fakeCli("feedback", log);
  const lifecycle = new LavishTurnLifecycle({ root, cli, clock: sequenceClock() });
  const result = await lifecycle.present(projection, { noOpen: true });
  const expectedPaths = artifactPaths(root, projection.focus.id, projection.review.id);
  assert.equal(result.paths.html, expectedPaths.html, "artifact path is deterministic by focus and review");
  assert.equal(result.metadata.status, "feedback");
  assert.equal(result.feedback.prompts.length, 2);
  assert.equal(await readFile(expectedPaths.html, "utf8"), html);
  const calls = (await readFile(log, "utf8")).trim().split("\n").map(JSON.parse);
  assert.deepEqual(calls.map(({ command }) => command), ["open", "poll"], "presentation opens then polls");

  const waitingLifecycle = new LavishTurnLifecycle({ root, cli: fakeCli("wait", log), clock: sequenceClock() });
  const controller = new AbortController();
  setTimeout(() => controller.abort(), 40);
  await assert.rejects(waitingLifecycle.resume(projection, { signal: controller.signal }), (error) => error?.name === "AbortError");
  const interrupted = JSON.parse(await readFile(expectedPaths.metadata, "utf8"));
  assert.equal(interrupted.status, "interrupted", "aborted polling keeps resumable presentation state");
  assert.equal(await readFile(expectedPaths.html, "utf8"), html, "abort preserves the artifact");

  const resumed = await lifecycle.resume(projection);
  assert.equal(resumed.metadata.status, "feedback", "interrupted path resumes the same Lavish session");

  const userEndedLifecycle = new LavishTurnLifecycle({ root, cli: fakeCli("ended", log), clock: sequenceClock() });
  const userEnded = await userEndedLifecycle.resume(projection);
  assert.equal(userEnded.metadata.status, "user_ended");
  await assert.rejects(userEndedLifecycle.resume(projection), /explicit reopen is required/, "user-ended sessions are not reopened implicitly");

  await lifecycle.cleanup(projection);
  await assert.rejects(readFile(expectedPaths.html, "utf8"), /ENOENT/);
  await assert.rejects(readFile(expectedPaths.metadata, "utf8"), /ENOENT/);

  const freshRoot = await mkdtemp(join(tmpdir(), "lavish-turn-resume-missing-"));
  try {
    const freshLog = join(freshRoot, "fake-cli.log");
    const fresh = new LavishTurnLifecycle({ root: freshRoot, cli: fakeCli("feedback", freshLog), clock: sequenceClock() });
    const freshResult = await fresh.resume(projection);
    assert.equal(freshResult.metadata.status, "feedback", "resume renders and opens when disposable files are missing");
    const freshCalls = (await readFile(freshLog, "utf8")).trim().split("\n").map(JSON.parse);
    assert.deepEqual(freshCalls.map(({ command }) => command), ["open", "poll"]);
  } finally { await rm(freshRoot, { recursive: true, force: true }); }
} finally {
  await rm(root, { recursive: true, force: true });
}

console.log("Lavish turn-renderer prototype OK: projection, shell, rich context, controls, feedback caps, CLI seam, resume/end, and cleanup verified.");

function fakeCli(mode, log) {
  return new LavishCliAdapter({
    command: process.execPath,
    argsPrefix: [join(fixtures, "fake-lavish-cli.mjs")],
    env: { FAKE_LAVISH_FIXTURES: fixtures, FAKE_LAVISH_POLL: mode, FAKE_LAVISH_LOG: log },
  });
}
function sequenceClock() {
  let tick = 0;
  return () => new Date(Date.UTC(2026, 6, 29, 0, 0, tick++)).toISOString();
}
