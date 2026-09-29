import { mkdtemp, mkdir, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { spawn } from "node:child_process";
import { EventEmitter } from "node:events";
import dagWorkflow from "../extensions/dag-workflow/index.ts";
import { ProjectModelDomain } from "../extensions/dag-workflow/project-model/domain.ts";
import { migrateLegacyBrainstorm } from "../extensions/dag-workflow/project-model/migration.ts";
import { bootstrapProjectMigration, migrationReadinessErrors } from "../extensions/dag-workflow/project-model/migration-workflow.ts";
import { validateProjectModel } from "../extensions/dag-workflow/project-model/model.ts";
import { FocusSessionStore, validateFocusSession } from "../extensions/dag-workflow/project-model/sessions.ts";
import { ProjectModelStore } from "../extensions/dag-workflow/project-model/store.ts";
import { LavishCliAdapter, parsePollOutput } from "../extensions/dag-workflow/project-model/lavish-cli.ts";
import { ReviewPresentationManager } from "../extensions/dag-workflow/project-model/review-presentation.ts";
import { renderReviewTurn } from "../extensions/dag-workflow/project-model/review-renderer.ts";

async function testDomainAndProjection() {
  await withTemp("domain", async (root) => {
    const domain = new ProjectModelDomain(root);
    await domain.models.initialize("demo", "Demo");
    const scope = { workstreamIds: [] };
    await domain.update(scope, {
      add: [
        { collection: "discoveries", key: "finding", value: base("Finding", "A useful finding.") },
        { collection: "proposals", key: "one", value: base("Option one", "Choose one.") },
        { collection: "proposals", key: "two", value: base("Option two", "Choose two.") },
      ],
    });
    const direction = await domain.recordDirection(scope, {
      directions: [{ collection: "intents", key: "goal", value: { ...base("Goal", "Build the thing."), kind: "outcome", relationships: [{ kind: "supports", targetId: "PROP-two" }, { kind: "challenges", targetId: "PROP-one" }] } }],
      currentUnderstanding: { body: "## Accepted direction\n\n- Build **the thing** for the accepted reason.\n- Keep `authority` explicit.\n\n<unsafe> stays text.", sourceObjectIds: ["INT-goal"] },
    });
    assert(!("receiptMode" in direction) && !(await domain.models.load()).intents[0].acceptance, "direct direction grants state without consent receipts");
    let malformedDirectionRejected = false;
    try { await domain.recordDirection(scope, { directions: [{ collection: "concepts", key: "bad-scope", value: { title: "Bad", body: "Bad scope.", scope: { kind: "workstreams", workstreamIds: 42 }, sourceRefs: [], relationships: [] } }] }); }
    catch (error) { malformedDirectionRejected = String(error.message).startsWith("Invalid project model:"); }
    assert(malformedDirectionRejected, "malformed direct authority rejects cleanly before persistence");
    await domain.update(scope, {
      specViews: [{ id: "SPEC-root", kind: "spec", path: "spec/spec.md", title: "Demo", sections: [{ id: "purpose", title: "Purpose", objectIds: ["INT-goal"] }] }],
    });
    const preview = await domain.specs({ action: "preview" });
    const candidate = await readFile(join(preview.directory, "spec/spec.md"), "utf8");
    assertIncludes(candidate, "**Candidate:**", "candidate projection marks uncutover content");
    assertIncludes(candidate, "Build the thing.", "candidate projection includes model-owned prose");

    let duplicateReviewRejected = false;
    try { await domain.createReview(scope, { title: "Duplicate", points: [{ key: "same", title: "A", context: "A.", purpose: "awareness" }, { key: "same!", title: "B", context: "B.", purpose: "awareness" }] }); }
    catch { duplicateReviewRejected = true; }
    assert(duplicateReviewRejected, "normalized duplicate review point IDs are rejected");
    const review = await domain.createReview(scope, {
      title: "Choose",
      points: [
        { key: "one", title: "First", context: "#### Why this decision exists\n\n- First **decision** with `context`.\n\n<unsafe-point> stays text.", purpose: "decision", question: "Choose one?", objectIds: ["PROP-one"], options: [{ key: "one", label: "One", description: "Choose one.", objectId: "PROP-one", direction: { collection: "decisions", key: "one", value: { title: "Choose one", body: "One is selected.", scope: { kind: "repository" }, sourceRefs: [], relationships: [], rationale: "It fits." } } }] },
        { key: "two", title: "Second", context: "WHY THIS DECISION EXISTS — Second decision. COST — More state.", purpose: "decision", question: "Choose two?", objectIds: ["PROP-two"], options: [{ key: "two", label: "Two", description: "Choose two.", objectId: "PROP-two", direction: { collection: "decisions", key: "two", value: { title: "Choose two", body: "Two is selected.", scope: { kind: "repository" }, sourceRefs: [], relationships: [], rationale: "It fits." } } }], rejectDirection: { collection: "decisions", key: "reject-two", value: { title: "Reject two", body: "Two is explicitly rejected.", scope: { kind: "repository" }, sourceRefs: [], relationships: [], rationale: "It does not fit." } } },
      ],
    });
    assertIncludes(review.markdown, "## Decisions needed", "review renders exact decision section");
    assertIncludes(review.markdown, "\"state\": \"accepted\"", "review renders the effective governing state");
    assertIncludes(review.markdown, "\"rationale\": \"It fits.\"", "review renders every semantic authority field");
    assertIncludes(review.markdown, "\"scope\"", "review renders materialized scope rather than an implicit default");

    const turn = await domain.reviewTurn(review.review.id);
    assert(turn.review.points.length === 2 && turn.review.revision === review.review.revision, "live turn projection binds the exact multi-point review hash");
    assert(turn.project.revision === (await domain.models.load()).project.revision && turn.project.modelHash.startsWith("sha256:"), "live turn projection carries current project identity");
    const renderedTurn = renderReviewTurn(turn);
    assert((renderedTurn.match(/class=\"decision-form\"/g) ?? []).length === 2, "production shell renders multiple independent decisions");
    assert((renderedTurn.match(/value=\"__other__\"/g) ?? []).length === 2, "production shell always renders Other");
    assert((renderedTurn.match(/name=\"responseText\"/g) ?? []).length === 2, "production shell keeps response text separate from radio choices");
    assert(!renderedTurn.includes("Exact authority payload") && !renderedTurn.includes("id=\"send-feedback\""), "production shell hides serialized authority and omits the redundant top send control");
    assertIncludes(renderedTurn, "<h4>Accepted direction</h4>", "Current understanding renders safe Markdown headings");
    assertIncludes(renderedTurn, "<li>Build <strong>the thing</strong> for the accepted reason.</li>", "Current understanding renders Markdown lists and emphasis");
    assertIncludes(renderedTurn, "<code>authority</code>", "Current understanding renders inline code");
    assert(renderedTurn.includes("&lt;unsafe&gt; stays text.") && !renderedTurn.includes("<unsafe>"), "Current understanding escapes raw HTML");
    assertIncludes(renderedTurn, "<h6>Why this decision exists</h6>", "review point context renders safe Markdown headings");
    assertIncludes(renderedTurn, "<li>First <strong>decision</strong> with <code>context</code>.</li>", "review point context renders Markdown lists and inline formatting");
    assert(renderedTurn.includes("&lt;unsafe-point&gt; stays text.") && !renderedTurn.includes("<unsafe-point>"), "review point context escapes raw HTML");
    assertIncludes(renderedTurn, "<h5>Why this decision exists</h5><p>Second decision.</p><h5>Cost</h5><p>More state.</p>", "legacy labeled review context renders as readable Markdown sections");

    const fakeLavish = join(root, "fake-lavish.mjs");
    await writeFile(fakeLavish, String.raw`const a=process.argv.slice(2);
const c=a[0]==="poll"||a[0]==="end"?a[0]:"open";
const f=c==="open"?a[0]:a[1];
if(c==="poll"&&process.env.FAKE_MODE==="wait")await new Promise(r=>setTimeout(r,30000));
if(c==="end"&&process.env.FAKE_MODE==="end-fail"){process.stderr.write("end failed");process.exit(2);}
if(c==="poll"&&process.env.FAKE_MODE==="collect-fail"){process.stderr.write("collect failed");process.exit(2);}
const session=(status,extra="")=>"session:\n  file: "+f+"\n  status: "+status+"\n"+extra;
if(c==="poll"&&process.env.FAKE_MODE==="ended")process.stdout.write(session("ended","  session_ended: true\n  ended_by: user\nprompts[0]:\nlayout_warnings[0]:\n"));
else if(c==="poll")process.stdout.write(session("feedback","prompts[1]:\n  - uid: \"1\"\n    prompt: \"Selected option\"\n    selector: \"#point-one\"\n    tag: model-review\n    text: \"Select One\"\nlayout_warnings[0]:\n"));
else process.stdout.write(session(c==="end"?"ended":"opened"));
`);
    const fakeDedicatedOpen = join(root, "fake-dedicated-open.mjs");
    const dedicatedOpenLog = join(root, "dedicated-open.json");
    await writeFile(fakeDedicatedOpen, String.raw`import { writeFile } from "node:fs/promises";
const args=process.argv.slice(2);
await writeFile(process.env.DEDICATED_OPEN_LOG,JSON.stringify(args));
process.stdout.write("session:\n  file: "+args[0]+"\n  status: opened\n");
`);
    const dedicatedAdapter = new LavishCliAdapter({
      command: process.execPath,
      argsPrefix: [fakeLavish],
      dedicatedOpenCommand: process.execPath,
      dedicatedOpenArgsPrefix: [fakeDedicatedOpen],
      env: { DEDICATED_OPEN_LOG: dedicatedOpenLog },
    });
    const dedicatedOpened = await dedicatedAdapter.open("/fixture/review.html", { reopen: true });
    assert(dedicatedOpened.status === "opened", "dedicated opener returns the Lavish session");
    assert(JSON.stringify(JSON.parse(await readFile(dedicatedOpenLog, "utf8"))) === JSON.stringify(["/fixture/review.html", "--reopen"]), "dedicated opener receives file and explicit reopen");
    await dedicatedAdapter.open("/fixture/headless.html", { noOpen: true });
    assert(JSON.stringify(JSON.parse(await readFile(dedicatedOpenLog, "utf8"))) === JSON.stringify(["/fixture/review.html", "--reopen"]), "no-open mode bypasses the dedicated browser helper");
    const missingDedicatedAdapter = new LavishCliAdapter({
      command: process.execPath,
      argsPrefix: [fakeLavish],
      dedicatedOpenCommand: join(root, "missing-lavish-open"),
    });
    const fallbackOpened = await missingDedicatedAdapter.open("/fixture/fallback.html");
    assert(fallbackOpened.status === "opened" && fallbackOpened.file === "/fixture/fallback.html", "missing dedicated opener falls back to the pinned/configured Lavish CLI");

    const feedbackManager = new ReviewPresentationManager(root, { cli: new LavishCliAdapter({ command: process.execPath, argsPrefix: [fakeLavish], env: { FAKE_MODE: "feedback" } }) });
    const phases = [];
    const presented = await feedbackManager.present(turn, { noOpen: true, onUpdate: ({ phase }) => phases.push(phase), onPresented: async () => { assert(await domain.markReviewPresented(turn.review.id, 0), "Lavish presentation metadata is informational"); } });
    assert(presented.metadata.status === "feedback" && presented.feedback.prompts.length === 1, "production lifecycle returns bounded fake-CLI feedback");
    assert(presented.metadata.artifactDigest === turn.review.artifactDigest, "presentation cache identity is separate from model direction");
    assert(phases.includes("rendered") && phases.includes("waiting") && phases.includes("feedback"), "production lifecycle reports long-running phases");
    assert(Boolean((await domain.models.load()).project.reviews[0].presentedAt), "presentation metadata is retained independently");
    const artifactBeforeCollect = await readFile(feedbackManager.paths(turn).html, "utf8");
    const collected = await feedbackManager.collect(turn);
    assert(collected.metadata.status === "feedback" && collected.feedback.prompts.length === 1, "nonblocking collection consumes already-submitted feedback");
    assert(await readFile(feedbackManager.paths(turn).html, "utf8") === artifactBeforeCollect, "feedback collection never rerenders the existing artifact");
    let duplicatePresentRejected = false;
    try { await feedbackManager.present(turn, { noOpen: true }); } catch (error) { duplicatePresentRejected = String(error.message).includes("already presented"); }
    assert(duplicatePresentRejected, "re-presenting an exact live review fails before replacing queued browser state");

    const waitManager = new ReviewPresentationManager(root, { cli: new LavishCliAdapter({ command: process.execPath, argsPrefix: [fakeLavish], env: { FAKE_MODE: "wait" } }) });
    const controller = new AbortController(); setTimeout(() => controller.abort(), 30);
    const waiting = waitManager.resume(turn, { signal: controller.signal });
    let concurrentCollectRejected = false;
    try { await waitManager.collect(turn); } catch (error) { concurrentCollectRejected = String(error.message).includes("already active"); }
    assert(concurrentCollectRejected, "concurrent presentation and collection cannot race or replace review metadata");
    let aborted = false;
    try { await waiting; } catch (error) { aborted = error?.name === "AbortError"; }
    assert(aborted, "aborting a production Lavish poll propagates AbortError");
    assert((await waitManager.readMetadata(waitManager.paths(turn)))?.status === "interrupted", "aborted production poll remains resumable");
    const resumed = await feedbackManager.resume(turn);
    assert(resumed.metadata.status === "feedback", "production poll resumes the stable artifact");
    const failingEndManager = new ReviewPresentationManager(root, { cli: new LavishCliAdapter({ command: process.execPath, argsPrefix: [fakeLavish], env: { FAKE_MODE: "end-fail" } }) });
    let failedEndRejected = false;
    try { await failingEndManager.end(turn); } catch { failedEndRejected = true; }
    assert(failedEndRejected && (await failingEndManager.readMetadata(failingEndManager.paths(turn)))?.status === "interrupted", "failed end never publishes stale ended authority");
    const failingDrainManager = new ReviewPresentationManager(root, { cli: new LavishCliAdapter({ command: process.execPath, argsPrefix: [fakeLavish], env: { FAKE_MODE: "collect-fail" } }) });
    let failedDrainRejected = false;
    try { await failingDrainManager.end(turn); } catch { failedDrainRejected = true; }
    const failedDrainMetadata = await failingDrainManager.readMetadata(failingDrainManager.paths(turn));
    assert(failedDrainRejected && failedDrainMetadata?.status === "interrupted" && !failedDrainMetadata.feedbackDrainedAt, "failed final drain never publishes stale ended authority");
    const agentEnded = await feedbackManager.end(turn);
    assert(agentEnded.metadata.status === "ended" && Boolean(agentEnded.metadata.feedbackDrainedAt) && agentEnded.feedback.prompts.length === 1, "successful end drains final queued feedback before replacement is allowed");

    const endedManager = new ReviewPresentationManager(root, { cli: new LavishCliAdapter({ command: process.execPath, argsPrefix: [fakeLavish], env: { FAKE_MODE: "ended" } }) });
    const endedPresentation = await endedManager.resume(turn);
    assert(endedPresentation.metadata.status === "user_ended", "user-ended Lavish state is durable");
    let implicitReopenRejected = false;
    try { await endedManager.resume(turn); } catch (error) { implicitReopenRejected = String(error.message).includes("explicit reopen"); }
    assert(implicitReopenRejected, "user-ended Lavish sessions do not resume implicitly");
    let implicitPresentRejected = false;
    try { await feedbackManager.present(turn, { noOpen: true }); } catch (error) { implicitPresentRejected = String(error.message).includes("ended by the user"); }
    assert(implicitPresentRejected, "user-ended Lavish sessions do not restart through present");
    await feedbackManager.cleanup(turn.review.id);

    const compactWarnings = parsePollOutput(`session:\n  file: /fixture/review.html\n  status: feedback\nprompts[0]:\nlayout_warnings[1]{selector,kind,axis,overflowPx,viewportWidth,severity,persistent}:\n  \"#review\",content-overflow,horizontal,50,390,error,true\n`);
    assert(compactWarnings.layoutWarnings.length === 1 && compactWarnings.layoutWarnings[0].persistent, "production parser recognizes compact TOON warning tables");

    const malformedReviewModel = structuredClone(await domain.models.load());
    malformedReviewModel.project.reviews[0].points[0].options[0].direction.value.scope = { kind: "workstreams", workstreamIds: 42 };
    assert(validateProjectModel(malformedReviewModel).some((error) => error.includes("direction")), "persisted review direction shapes validate");
    await domain.update(scope, { patch: [{ id: "PROP-two", changes: { body: "Changed after presentation." } }] });
    const resolved = await domain.resolveReview({ reviewId: review.review.id, expectedRevision: 1, outcomes: [{ pointId: "point-one", action: "accept", optionId: "option-one" }] });
    assert(resolved.appliedPointIds.includes("point-one") && resolved.unresolvedPointIds.includes("point-two"), "sparse outcomes leave omitted points pending");
    assert(!(await domain.models.load()).decisions[0].acceptance, "review does not fabricate acceptance receipts");
    const revisionBefore = (await domain.models.load()).project.revision;
    let staleRevisionRejected = false;
    try { await domain.resolveReview({ reviewId: review.review.id, expectedRevision: 1, outcomes: [{ pointId: "point-two", action: "reject" }] }); } catch (error) { staleRevisionRejected = error.message.includes("revision conflict"); }
    assert(staleRevisionRejected && (await domain.models.load()).project.revision === revisionBefore, "exact review CAS rejects stale writers atomically");
    await domain.resolveReview({ reviewId: review.review.id, expectedRevision: 2, outcomes: [{ pointId: "point-two", action: "reject" }] });
    assert((await domain.models.load()).project.reviews[0].status === "resolved", "resolved record and outcomes remain durable");
    await domain.update(scope, { specViews: [{ id: "SPEC-root", kind: "spec", path: "spec/spec.md", title: "Demo", sections: [{ id: "purpose", title: "Purpose", objectIds: ["INT-goal", "DEC-one", "DEC-reject-two"] }] }] });
    const model = await domain.models.load();
    const malformed = structuredClone(model);
    malformed.project.projections.specs[0].kind = "garbage";
    malformed.project.projections.specs[0].sections = "bad";
    malformed.project.projections.specs[0].childViewIds = 42;
    const malformedErrors = validateProjectModel(malformed);
    assert(malformedErrors.some((error) => error.includes("kind is invalid")) && malformedErrors.some((error) => error.includes("sections must be an array")), "runtime validation reports malformed projection shapes without crashing");
    assert(validateProjectModel(null).includes("project model must be an object"), "runtime validation rejects a null model without crashing");
    const nullProject = structuredClone(model); nullProject.project = null;
    assert(validateProjectModel(nullProject).some((error) => error.includes("project.id")), "runtime validation rejects null project metadata without crashing");
    const nullRelationship = structuredClone(model); nullRelationship.intents[0].relationships = [null];
    assert(validateProjectModel(nullRelationship).some((error) => error.includes("relationship must be an object")), "runtime validation rejects null relationship members without crashing");
    const malformedScope = structuredClone(model);
    malformedScope.intents[0].scope = { kind: "workstreams", workstreamIds: 42 };
    assert(validateProjectModel(malformedScope).some((error) => error.includes("workstreamIds")), "runtime validation rejects malformed scope arrays without crashing");
    const malformedTension = structuredClone(model);
    malformedTension.tensions.push({ ...base("Bad tension", "Malformed poles."), id: "TEN-bad", state: "active", createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(), poleObjectIds: 42 });
    assert(validateProjectModel(malformedTension).some((error) => error.includes("poleObjectIds")), "runtime validation rejects malformed tension arrays without crashing");
    let malformedFocusRejected = false;
    try { validateFocusSession({ schemaVersion: 1, id: 42, title: 42, workstreamIds: [], createdAt: "bad", updatedAt: "bad", status: "active", previousReview: { modelHash: "x", projectionVersion: 1, workstreamIds: [], objects: 42, presentedAt: "bad" } }); }
    catch (error) { malformedFocusRejected = String(error.message).startsWith("Invalid focus session:"); }
    assert(malformedFocusRejected, "focus validation rejects malformed runtime shapes without raw type errors");
    await mkdir(join(root, "spec"), { recursive: true });
    await writeFile(join(root, "spec/spec.md"), "# Existing hand-maintained spec\n");
    let unsafeReplacementRejected = false;
    try { await domain.cutover(scope); } catch { unsafeReplacementRejected = true; }
    assert(unsafeReplacementRejected, "cutover without prepared migration metadata cannot replace unmanaged files");
    await rm(join(root, "spec/spec.md"));
    const cutover = await domain.cutover(scope);
    assert(cutover.action === "migration_cutover" && !(await domain.models.load()).intents[0].acceptance, "cutover sets governing state without semantic proof");
    assertIncludes(await readFile(join(root, "spec/spec.md"), "utf8"), "generated-by: pi-dag-workflow/project-model", "cutover generates declared absent projection targets");

    await domain.recordDirection(scope, {
      directions: [{
        collection: "decisions",
        key: "one-v2",
        value: {
          ...base("Choose one v2", "One is replaced by the current direction."),
          rationale: "The newer direction supersedes the older accepted decision.",
          relationships: [{ kind: "supersedes", targetId: "DEC-one" }],
        },
      }],
      specViews: [{ id: "SPEC-root", kind: "spec", path: "spec/spec.md", title: "Demo", sections: [{ id: "purpose", title: "Purpose", objectIds: ["INT-goal", "DEC-one-v2", "DEC-reject-two"] }] }],
    });
    const supersededProjection = await readFile(join(root, "spec/spec.md"), "utf8");
    assertIncludes(supersededProjection, "One is replaced by the current direction.", "accepted superseder renders as governing direction");
    assert(!supersededProjection.includes("One is selected."), "superseded accepted decision stops rendering");
    const governing = await domain.context(scope, { view: "governing" });
    assert(!governing.some(({ object }) => object.id === "DEC-one") && governing.some(({ object }) => object.id === "DEC-one-v2"), "governing context excludes superseded accepted decisions");
    assert(validateProjectModel(await domain.models.load()).length === 0, "superseded accepted objects no longer require canonical generated-spec placement");

    await domain.update(scope, {
      add: [{ collection: "questions", key: "reconsider", value: { ...base("Reconsider goal", "Should the goal change?"), kind: "reconsideration", relationships: [{ kind: "challenges", targetId: "INT-goal" }] } }],
    });
    const generated = await readFile(join(root, "spec/spec.md"), "utf8");
    assertIncludes(generated, "**Under review:**", "non-authoritative reconsideration synchronizes current spec marker");
    const check = await domain.specs({ action: "check" });
    assert(check.ok, "generated specs check cleanly");

    const beforeCollision = await domain.models.load();
    await writeFile(join(root, "spec/spec.md"), "# Human file\n");
    let collisionRejected = false;
    try { await domain.update(scope, { add: [{ collection: "discoveries", key: "collision", value: base("Collision", "Must roll back.") }] }); }
    catch { collisionRejected = true; }
    assert(collisionRejected, "unmanaged generated-spec collision rejects the semantic transaction");
    assert((await domain.models.load()).project.revision === beforeCollision.project.revision, "projection failure leaves model authority unchanged");
    assert((await readFile(join(root, "spec/spec.md"), "utf8")) === "# Human file\n", "projection rollback preserves collided human content");
    await domain.specs({ action: "generate", replaceUnmanaged: true });

    let rejected = false;
    try { await domain.update(scope, { patch: [{ id: "INT-goal", changes: { body: "Unauthorized rewrite." } }] }); }
    catch { rejected = true; }
    assert(rejected, "ordinary update cannot rewrite accepted semantics");
    assert(validateProjectModel(await domain.models.load()).length === 0, "stored authoritative model remains valid");

    const beforeRejectedDirection = await domain.models.load();
    let unroutedRejected = false;
    try { await domain.recordDirection(scope, { directions: [{ collection: "commitments", key: "unrouted", value: base("Unrouted", "Must not partially commit.") }] }); }
    catch { unroutedRejected = true; }
    assert(unroutedRejected, "authoritative direction without canonical placement is rejected");
    assert((await domain.models.load()).project.revision === beforeRejectedDirection.project.revision, "failed authority mutation leaves model revision unchanged");

    let forged = false;
    try {
      await domain.update(scope, { add: [{ collection: "intents", key: "forged", value: { ...base("Forged", "Not human authority."), kind: "outcome", state: "accepted", acceptance: { actor: "user", mode: "direct_direction", acceptedAt: new Date().toISOString(), interactionRef: "fake", contentHash: "sha256:" + "0".repeat(64) } } }] });
    } catch { forged = true; }
    assert(forged, "ordinary update cannot forge acceptance fields or governing state");
  });
}

async function testProcessSharedPersistence() {
  await withTemp("process-shared", async (root) => {
    const models = new ProjectModelStore(root);
    const sessions = new FocusSessionStore(root);
    await models.initialize("concurrent", "Concurrent");
    const child = join(root, "persistence-child.mjs");
    const storeUrl = new URL("../extensions/dag-workflow/project-model/store.ts", import.meta.url).href;
    await writeFile(child, `import { ProjectModelStore } from ${JSON.stringify(storeUrl)};\nconst [mode, root, value] = process.argv.slice(2);\nconst store = new ProjectModelStore(root);\nawait store.mutate(async (draft) => { await new Promise((resolve) => setTimeout(resolve, 25)); draft.project.title += "|" + value; });\n`);

    await Promise.all(Array.from({ length: 8 }, (_, index) => runProcess(child, ["model", root, String(index)])));
    const concurrentModel = await models.load();
    assert(concurrentModel.project.revision === 8, "process-shared model lock preserves every concurrent revision");
    assert(Array.from({ length: 8 }, (_, index) => concurrentModel.project.title.includes(`|${index}`)).every(Boolean), "process-shared model mutations do not lose concurrent values");

    /* Independent review concurrency is exercised in the focus-free feature suite. */
    const staleModel = structuredClone(await models.load());
    await models.mutate((draft) => { draft.project.title += "|fresh"; });
    staleModel.project.revision += 1;
    let staleModelRejected = false;
    try { await models.write(staleModel); } catch (error) { staleModelRejected = String(error.message).includes("revision conflict"); }
    assert(staleModelRejected, "model write compares its expected integer revision under the lock");

    const historicalId = "focus-historical";
    const historicalPath = sessions.path(historicalId);
    const timestamp = new Date().toISOString();
    await mkdir(join(root, ".ai/model-sessions"), { recursive: true });
    await writeFile(historicalPath, `${JSON.stringify({ schemaVersion: 1, id: historicalId, title: "Historical", workstreamIds: [], createdAt: timestamp, updatedAt: timestamp, status: "active" }, null, 2)}\n`);
    assert((await sessions.load(historicalId)).revision === 0, "historical focus files without a revision remain readable");
    assert(sessions.mutate === undefined && sessions.write === undefined && sessions.create === undefined, "historical focus access is read-only");

    const beforeCrash = await models.load();
    const tempCrash = await runProcess(child, ["model-crash", root, "temp-crash"], { PI_PROJECT_MODEL_TEST_CRASH_POINT: "after-temp-fsync" }, true);
    assert(tempCrash.signal === "SIGKILL", "temp-fsync crash point terminates the writer process");
    const afterTempCrash = await models.load();
    assert(afterTempCrash.project.revision === beforeCrash.project.revision && afterTempCrash.project.title === beforeCrash.project.title, "crash after temp fsync leaves the prior snapshot intact");
    await models.mutate((draft) => { draft.project.title += "|recovered-temp"; });
    assert((await models.load()).project.title.includes("|recovered-temp"), "PID/start identity recovery retires a crashed writer lock");

    const beforeRenameCrash = await models.load();
    const renameCrash = await runProcess(child, ["model-crash", root, "rename-crash"], { PI_PROJECT_MODEL_TEST_CRASH_POINT: "after-rename" }, true);
    assert(renameCrash.signal === "SIGKILL", "post-rename crash point terminates the writer process");
    const afterRenameCrash = await models.load();
    assert(afterRenameCrash.project.revision === beforeRenameCrash.project.revision + 1 && afterRenameCrash.project.title.includes("|rename-crash"), "renamed snapshot is complete and readable after a writer crash");
    await models.mutate((draft) => { draft.project.title += "|recovered-rename"; });
    assert((await models.load()).project.title.includes("|recovered-rename"), "stale lock recovery also succeeds after atomic rename");

    await mkdir(join(root, "redirected-focus"));
    await symlink(join(root, "redirected-focus"), join(root, "linked-focus"));
    let focusSymlinkRejected = false;
    try { await new FocusSessionStore(root, "linked-focus").load("focus-redirected"); }
    catch (error) { focusSymlinkRejected = String(error.message).includes("symlink"); }
    assert(focusSymlinkRejected, "focus persistence refuses a repository path redirected through a symlink");

    const rootAlias = `${root}-ancestor-alias`;
    await symlink(root, rootAlias);
    let modelRootAliasRejected = false;
    let focusRootAliasRejected = false;
    try { new ProjectModelStore(rootAlias); } catch (error) { modelRootAliasRejected = String(error.message).includes("symlink"); }
    try { new FocusSessionStore(rootAlias); } catch (error) { focusRootAliasRejected = String(error.message).includes("symlink"); }
    await rm(rootAlias, { force: true });
    assert(modelRootAliasRejected && focusRootAliasRejected, "model and focus stores reject repository roots reached through symlink ancestors");
  });
}

async function testMigration() {
  const legacy = {
    id: "legacy", title: "Legacy", neighborhoods: [{ id: "area", title: "Area", status: "active" }],
    tangents: [], questions: [], evidence: [], proposals: [], probes: [], promotions: [],
    decisions: [{ id: "D-old", title: "Keep behavior", contract: "Behavior remains.", rationale: "Needed.", status: "active", questionIds: [] }],
  };
  await withTemp("migration", async (root) => {
    await mkdir(join(root, "spec"), { recursive: true });
    await writeFile(join(root, "spec/spec.md"), "# Legacy\n");
    const migrated = await migrateLegacyBrainstorm(root, legacy);
    assert(migrated.model.project.mode === "candidate", "migration creates candidate mode");
    assert(migrated.model.commitments.length === 1, "legacy behavior maps to candidate commitment");
    assertIncludes(migrated.report, "pending semantic audit", "migration report requires audit");
    assert(validateProjectModel(migrated.model).length === 0, "migration candidate validates");
  });
  await withTemp("legacy-fast-path", async (root) => {
    await mkdir(join(root, ".ai/brainstorm"), { recursive: true });
    await mkdir(join(root, "spec"), { recursive: true });
    await writeFile(join(root, ".ai/brainstorm/structured-brainstorming.json"), JSON.stringify(legacy));
    await writeFile(join(root, "spec/spec.md"), "# Legacy remains untouched\n");
    const bootstrap = await bootstrapProjectMigration(root);
    assert(bootstrap.usedLegacyAdapter, "/dag migrate uses the supported deterministic legacy adapter as a fast path");
    assert((await new ProjectModelStore(root).load()).project.title === "Legacy", "legacy fast path preserves the migrated repository title");
    assertIncludes(await readFile(join(root, "project-model/migrations/brainstorm-v2-candidate.md"), "utf8"), "Object mapping", "legacy fast path retains its mapping audit");
    assert(await readFile(join(root, "spec/spec.md"), "utf8") === "# Legacy remains untouched\n", "legacy fast path previews without replacing existing specs");
  });
}

async function testGuidedMigrationWorkflow() {
  await withTemp("guided-migration", async (root) => {
    await mkdir(join(root, "spec"), { recursive: true });
    await writeFile(join(root, "package.json"), '{"name":"migration-demo"}\n');
    await writeFile(join(root, "README.md"), "# Migration Demo\n");
    await writeFile(join(root, "spec/spec.md"), "# Existing index\n");
    await writeFile(join(root, "spec/manual.md"), "# Required manual contract\n");

    const initialFiles = new Map([
      ["spec/spec.md", await readFile(join(root, "spec/spec.md"), "utf8")],
      ["spec/manual.md", await readFile(join(root, "spec/manual.md"), "utf8")],
    ]);
    const bootstrap = await bootstrapProjectMigration(root);
    assert(bootstrap.created && !bootstrap.usedLegacyAdapter, "guided migration bootstraps a generic candidate");
    const domain = new ProjectModelDomain(root);
    const scope = { workstreamIds: [] };
    let candidate = await domain.models.load();
    assert(candidate.project.mode === "candidate" && candidate.project.migration?.phase === "inventory", "bootstrap persists candidate migration metadata");
    assert(candidate.project.migration.sources.some(({ path }) => path === "README.md"), "relevant-first inventory includes repository orientation");
    assert(candidate.project.migration.artifacts.some(({ path, disposition }) => path === "spec/manual.md" && disposition === "unresolved"), "existing specs begin unresolved");
    for (const [path, content] of initialFiles) assert(await readFile(join(root, path), "utf8") === content, "bootstrap never overwrites existing specs");

    const resumed = await bootstrapProjectMigration(root);
    assert(!resumed.created && !resumed.focusId, "repeated /dag migrate resumes the candidate without focus");

    const sources = candidate.project.migration.sources.map((source) => ({
      path: source.path,
      kind: source.kind,
      disposition: source.path === "README.md" ? "mapped" : source.path === "spec/manual.md" ? "retained" : "omitted",
      reason: source.path === "README.md" ? "Mapped into the candidate intent." : source.path === "spec/manual.md" ? "Required manual reference." : "Orientation or superseded index does not add governing meaning.",
    }));
    let incompleteRejected = false;
    try {
      await domain.update(scope, {
        migration: {
          phase: "ready",
          sources,
          artifacts: candidate.project.migration.artifacts.map(({ path }) => ({ path, disposition: "unresolved" })),
          blockers: [],
        },
      });
    } catch (error) { incompleteRejected = String(error.message).includes("not cutover-ready"); }
    assert(incompleteRejected, "ready phase rejects unresolved artifacts and absent semantic projections");

    await domain.update(scope, {
      add: [
        { collection: "workstreams", key: "product", value: { ...base("Product", "Migrated product behavior."), state: "active" } },
        { collection: "intents", key: "goal", value: { ...base("Migration goal", "Preserve the product's current supported behavior."), kind: "outcome", sourceRefs: ["README.md"] } },
      ],
      specViews: [
        { id: "SPEC-root", kind: "index", path: "spec/spec.md", title: "Product specifications", childViewIds: ["SPEC-product"], manualLinks: [{ path: "spec/manual.md", title: "Manual contract", summary: "Required retained reference." }] },
        { id: "SPEC-product", kind: "spec", path: "spec/generated.md", title: "Product", sections: [{ id: "intent", title: "Intent", objectIds: ["INT-goal"] }] },
      ],
      migration: {
        phase: "ready",
        sources,
        artifacts: [
          { path: "spec/spec.md", disposition: "replace_generated", reason: "Replace the approved index collision." },
          { path: "spec/generated.md", disposition: "create_generated", reason: "Create the approved generated product view." },
          { path: "spec/manual.md", disposition: "retain_reference", reason: "Keep the required manual contract side by side." },
        ],
        blockers: [],
      },
    });
    candidate = await domain.models.load();
    assert(candidate.project.migration.phase === "ready" && migrationReadinessErrors(candidate).length === 0, "complete semantic and artifact dispositions make the candidate ready");
    assert(await readFile(join(root, "spec/spec.md"), "utf8") === initialFiles.get("spec/spec.md"), "ready candidate still does not overwrite an approved collision");

    await domain.update(scope, { patch: [{ id: "INT-goal", changes: { sourceRefs: [] } }] });
    candidate = await domain.models.load();
    let untraceableRejected = false;
    try { await domain.cutover(scope); }
    catch (error) { untraceableRejected = String(error.message).includes("lacks source traceability"); }
    assert(untraceableRejected, "cutover rejects governing candidate meaning without source traceability");
    await domain.update(scope, { patch: [{ id: "INT-goal", changes: { sourceRefs: ["README.md"] } }] });
    candidate = await domain.models.load();
    await writeFile(join(root, "README.md"), "# Changed after review\n");
    let sourceDriftRejected = false;
    try { await domain.cutover(scope); }
    catch (error) { sourceDriftRejected = String(error.message).includes("source changed after inventory"); }
    assert(sourceDriftRejected, "cutover rejects source drift after review");
    await writeFile(join(root, "README.md"), "# Migration Demo\n");

    await domain.update(scope, { add: [{ collection: "discoveries", key: "post-review", value: base("Post-review finding", "This changes the exact candidate manifest.") }] });
    assert(!(await domain.models.load()).project.migration.focusId, "migration metadata has no active focus authority");

    const coexistenceReview = await domain.createReview(scope, {
      title: "Migration coexistence",
      points: [{ key: "coexist", title: "Keep refining", context: "Retain a manual artifact side by side.", purpose: "decision", question: "Continue refining?", options: [{ key: "continue", label: "Continue", description: "Keep the candidate non-authoritative." }] }],
    });
    const coexistence = await domain.resolveReview({ reviewId: coexistenceReview.review.id, expectedRevision: 0, outcomes: [{ pointId: "point-coexist", action: "accept", optionId: "option-continue" }] });
    assert(!coexistence.unresolvedPointIds.length && (await domain.models.load()).project.reviews.find(r => r.id === coexistenceReview.review.id).status === "resolved", "directionless migration feedback can close a non-authoritative refinement choice");

    const review = await domain.createReview(scope, {
      title: "Migration cutover",
      points: [{ key: "cutover", title: "Cut over", context: "Review the candidate and artifact dispositions.", purpose: "decision", question: "Cut over?", options: [{ key: "yes", label: "Cut over", description: "Apply the exact candidate." }] }],
    });
    await domain.resolveReview({ reviewId: review.review.id, expectedRevision: 0, outcomes: [{ pointId: "point-cutover", action: "accept", optionId: "option-yes" }] });
    candidate = await domain.models.load();
    await domain.cutover(scope);
    const authoritative = await domain.models.load();
    assert(authoritative.project.mode === "authoritative", "fresh ready candidate cuts over atomically");
    assert((await domain.models.load()).project.reviews.find(r => r.id === review.review.id).status === "resolved", "cutover retains the explicitly resolved review");
    assertIncludes(await readFile(join(root, "spec/spec.md"), "utf8"), "generated-by: pi-dag-workflow/project-model", "cutover replaces only the approved projection collision");
    assertIncludes(await readFile(join(root, "spec/generated.md"), "utf8"), "Preserve the product", "cutover creates approved generated projections");
    assert(await readFile(join(root, "spec/manual.md"), "utf8") === initialFiles.get("spec/manual.md"), "cutover preserves required side-by-side specs byte-for-byte");
    let authoritativeRejected = false;
    try { await bootstrapProjectMigration(root); }
    catch (error) { authoritativeRejected = String(error.message).includes("already has an authoritative"); }
    assert(authoritativeRejected, "/dag migrate fails closed for an authoritative model");
  });
}

async function testPiMigrationCommand() {
  await withTemp("pi-migration", async (root) => {
    await writeFile(join(root, "package.json"), '{"name":"pi-migration"}\n');
    await writeFile(join(root, "README.md"), "# Pi Migration\n");
    const pi = new FakePi();
    const workerRoleEnvironment = Object.fromEntries(Object.entries(process.env).filter(([key]) => key.startsWith("PI_DAG_WORKER_")));
    for (const key of Object.keys(workerRoleEnvironment)) delete process.env[key];
    try { dagWorkflow(pi); }
    finally { for (const [key, value] of Object.entries(workerRoleEnvironment)) process.env[key] = value; }
    pi.loading = false;
    const ctx = pi.context(root);
    await pi.runCommand("dag", "migrate", ctx);
    assert(pi.activeTools.has("dag_model_context"), "/dag migrate activates the existing model tools");
    const model = await new ProjectModelStore(root).load();
    assert(model.project.mode === "candidate" && !model.project.migration?.focusId, "/dag migrate creates the candidate without focus");
    assertIncludes(pi.messages.at(-1).content, "Lavish", "/dag migrate supports channel-neutral review");
    const [promptUpdate] = (await pi.emit("before_agent_start", { systemPrompt: "Base prompt" }, ctx)).filter(Boolean);
    assertIncludes(pi.messages.at(-1).content, "guided project-model migration mode", "migration mode receives dedicated inference guidance");
    assertIncludes(pi.messages.at(-1).content, "Physical coexistence is allowed; dual semantic authority is not", "migration guidance preserves the accepted coexistence boundary");
  });
}

async function testPiIntegration() {
  await withTemp("pi", async (root) => {
    const domain = new ProjectModelDomain(root);
    await domain.models.initialize("demo", "Demo");
    const pi = new FakePi();
    const workerRoleEnvironment = Object.fromEntries(Object.entries(process.env).filter(([key]) => key.startsWith("PI_DAG_WORKER_")));
    for (const key of Object.keys(workerRoleEnvironment)) delete process.env[key];
    try { dagWorkflow(pi); }
    finally { for (const [key, value] of Object.entries(workerRoleEnvironment)) process.env[key] = value; }
    pi.loading = false;
    const ctx = pi.context(root);
    await pi.emit("session_start", { reason: "startup" }, ctx);
    await pi.callTool("dag_model_context", { scope: { workstreamIds: [] } }, ctx);
    await pi.callTool("dag_model_update", { scope: { workstreamIds: [] }, add: [{ collection: "discoveries", key: "before-brainstorm", value: base("Available", "Tools work without a command or focus.") }] }, ctx);
    assert((await domain.models.load()).discoveries.length === 1 && (await domain.sessions.list()).length === 0, "Pi context and CRUD work before any brainstorm command");
    await pi.runCommand("dag", "brainstorm Schema topic", ctx);
    assert(pi.activeTools.has("dag_model_context"), "brainstorm command activates model tools");
    const [promptUpdate] = (await pi.emit("before_agent_start", { systemPrompt: "Base prompt" }, ctx)).filter(Boolean);
    assertIncludes(promptUpdate.systemPrompt, "Write question briefs as scannable Markdown", "brainstorm mode injects structured question-brief guidance");
    assertIncludes(promptUpdate.systemPrompt, "selective causal synthesis", "brainstorm mode distinguishes comprehension from template completion");
    assertIncludes(promptUpdate.systemPrompt, "automatically explore and present the next supported material frontier", "brainstorm mode continues through supported frontiers");
    assertIncludes(promptUpdate.systemPrompt, "Do not require a distinct formal acknowledgement surface", "brainstorm mode leaves acknowledgements conversational");
    assert(["subagent", "subagent_status", "subagent_inspect", "subagent_tail", "subagent_cancel", "subagent_retry"].every((name) => pi.tools.has(name)), "generic owned-worker tools are registered independently of DAG mode");
    assert(pi.commands.has("workers"), "generic /workers command is registered");
    assert(!pi.tools.has("dag_init") && !pi.tools.has("dag_start_node"), "mutating legacy DAG tools are not registered");
    assert(![...pi.tools].some(([name]) => name.startsWith("dag_grillme")), "GrillMe tools are absent");
    try {
      const scope = { workstreamIds: [] };
      await pi.callTool("dag_model_record_direction", { scope, directions: [{ collection: "intents", key: "goal", value: { ...base("Goal", "Use this contextual direction."), kind: "outcome" } }] }, ctx);
      await pi.emit("agent_settled", {}, ctx);
      await pi.callTool("dag_model_update", { scope, specViews: [{ id: "SPEC-root", kind: "spec", path: "spec/spec.md", title: "Demo", sections: [{ id: "purpose", title: "Purpose", objectIds: ["INT-goal"] }] }] }, ctx);
      await pi.callTool("dag_model_record_direction", { scope, cutover: true }, ctx);
      assert((await domain.models.load()).project.mode === "authoritative", "cutover does not require an ephemeral current-turn marker");
      const reviewTool = await pi.callTool("dag_model_review", { scope, title: "Awareness", points: [{ key: "notice", title: "Notice", context: "Context packet.", purpose: "awareness" }] }, ctx);
      assert(reviewTool.details.reviewId === "review-awareness" && reviewTool.details.reviewRevision === 0, "review result returns an independent address and CAS revision");
      const secondReview = await pi.callTool("dag_model_review", { scope, title: "Second", points: [{ key: "notice", title: "Notice", context: "Separate packet.", purpose: "awareness" }] }, ctx);
      await pi.emit("agent_settled", {}, ctx);
      await pi.callTool("dag_model_resolve_review", { reviewId: reviewTool.details.reviewId, expectedRevision: 0, outcomes: [{ pointId: "point-notice", action: "accept" }] }, ctx);
      assert((await domain.pendingReviews(scope)).some(record => record.reviewId === secondReview.details.reviewId), "Pi resolves the exact review without an ephemeral user marker or selecting a singleton");
      assert((await domain.sessions.list()).length === 0 && pi.entries.every(entry => entry.customType !== "dag-model-focus-link"), "brainstorm does not create or link a durable focus");
      await pi.emit("session_start", { reason: "fork" }, ctx);
      assert(pi.activeTools.has("dag_model_context"), "fork leaves model tools available without focus restoration");
      await withTemp("other-repository", async (otherRoot) => {
        const otherCtx = pi.context(otherRoot);
        await pi.emit("session_start", { reason: "switch-repository" }, otherCtx);
        assert(pi.activeTools.has("dag_model_context"), "model tools remain enabled in another repository");
      });
      await pi.runCommand("dag", "brainstorm resume focus-schema-focus", ctx);
      await pi.runCommand("dag", "plan", ctx);
      assert(pi.activeTools.has("dag_model_context"), "planning leaves model tools available without focus");
      assert(!ctx.ui.notifications.some(({ message }) => message.includes("deferred")), "planning is routed to the product integration rather than the legacy deferral");
      await pi.runCommand("dag", "brainstorm stop", ctx);
      await pi.emit("session_start", { reason: "reload" }, ctx);
      assert(pi.activeTools.has("dag_model_context") && pi.activeTools.has("dag_model_present_review"), "suspension does not disable model tools");
    } finally {
      await pi.emit("session_shutdown", { reason: "quit" }, ctx);
      assert(pi.bus.eventNames().length === 0, "session shutdown removes bus subscriptions");
    }
  });
}

function base(title, body) {
  return { title, body, scope: { kind: "repository" }, introducedBy: "agent", sourceRefs: [], relationships: [] };
}

class FakePi {
  bus = new EventEmitter();
  events = {
    emit: (name, data) => { this.bus.emit(name, data); },
    on: (name, listener) => {
      const handler = (data) => listener(data);
      this.bus.on(name, handler);
      return () => { this.bus.off(name, handler); };
    },
  };
  constructor() {
    this.tools = new Map(); this.commands = new Map(); this.handlers = new Map(); this.activeTools = new Set(["read", "bash"]); this.entries = []; this.messages = []; this.loading = true;
  }
  registerTool(tool) { this.tools.set(tool.name, tool); this.activeTools.add(tool.name); }
  registerCommand(name, command) { this.commands.set(name, command); }
  on(event, handler) { const list = this.handlers.get(event) ?? []; list.push(handler); this.handlers.set(event, list); }
  getActiveTools() { return [...this.activeTools]; }
  setActiveTools(names) { if (this.loading) throw new Error("Extension runtime not initialized"); this.activeTools = new Set(names); }
  appendEntry(customType, data) { this.entries.push({ type: "custom", customType, data }); }
  sendMessage(message) { this.messages.push(message); }
  async emit(event, payload, ctx) {
    const results = [];
    for (const handler of this.handlers.get(event) ?? []) results.push(await handler(payload, ctx));
    return results;
  }
  async runCommand(name, args, ctx) { return this.commands.get(name).handler(args, ctx); }
  async callTool(name, params, ctx) {
    if (!this.activeTools.has(name)) throw new Error(`inactive tool ${name}`);
    return this.tools.get(name).execute("call", params, undefined, undefined, ctx);
  }
  context(cwd) {
    const notifications = [];
    return {
      cwd, hasUI: true, mode: "tui",
      sessionManager: { getBranch: () => this.entries, getEntries: () => this.entries, getSessionId: () => "pi-session" },
      ui: {
        notifications,
        notify: (message, level = "info") => notifications.push({ message, level }),
        select: async (_title, values) => values[0],
        input: async () => "Focus",
      },
    };
  }
}

function runProcess(script, args, env = {}, allowSignal = false) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [script, ...args], { env: { ...process.env, ...env }, stdio: ["ignore", "pipe", "pipe"] });
    let stdout = ""; let stderr = "";
    child.stdout.on("data", (chunk) => { stdout += chunk; });
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    child.on("error", reject);
    child.on("close", (code, signal) => {
      if (code === 0 || (allowSignal && signal)) resolve({ code, signal, stdout, stderr });
      else reject(new Error(`Persistence child failed (${code ?? signal}): ${stderr || stdout}`));
    });
  });
}

async function withTemp(name, fn) {
  const root = await mkdtemp(join(tmpdir(), `pi-model-${name}-`));
  try { await fn(root); } finally { await rm(root, { recursive: true, force: true }); }
}
function assertIncludes(text, value, message) { assert(text.includes(value), `${message}: missing ${value}`); }
function assert(value, message) { if (!value) throw new Error(`Project model test failed: ${message}`); }

await testDomainAndProjection();
await testProcessSharedPersistence();
await testMigration();
await testGuidedMigrationWorkflow();
await testPiMigrationCommand();
await testPiIntegration();
console.log("Project model production tests OK");

await import("./project-model-focus-free-test.mjs");
