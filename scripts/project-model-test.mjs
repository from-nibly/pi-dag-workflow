import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import dagWorkflow from "../extensions/dag-workflow/index.ts";
import { ProjectModelDomain } from "../extensions/dag-workflow/project-model/domain.ts";
import { migrateLegacyBrainstorm } from "../extensions/dag-workflow/project-model/migration.ts";
import { candidateManifestHash, validateProjectModel } from "../extensions/dag-workflow/project-model/model.ts";
import { validateFocusSession } from "../extensions/dag-workflow/project-model/sessions.ts";
import { LavishCliAdapter, parsePollOutput } from "../extensions/dag-workflow/project-model/lavish-cli.ts";
import { ReviewPresentationManager } from "../extensions/dag-workflow/project-model/review-presentation.ts";
import { renderReviewTurn } from "../extensions/dag-workflow/project-model/review-renderer.ts";
import { lavishFeedbackInteractionRef } from "../extensions/dag-workflow/project-model/integration.ts";

async function testDomainAndProjection() {
  await withTemp("domain", async (root) => {
    const domain = new ProjectModelDomain(root);
    await domain.models.initialize("demo", "Demo");
    const focus = await domain.sessions.create({ title: "Schema focus" });
    await domain.update(focus.id, {
      add: [
        { collection: "discoveries", key: "finding", value: base("Finding", "A useful finding.") },
        { collection: "proposals", key: "one", value: base("Option one", "Choose one.") },
        { collection: "proposals", key: "two", value: base("Option two", "Choose two.") },
      ],
    });
    const direction = await domain.recordDirection(focus.id, {
      directions: [{ collection: "intents", key: "goal", value: { ...base("Goal", "Build the thing."), kind: "outcome", relationships: [{ kind: "supports", targetId: "PROP-two" }, { kind: "challenges", targetId: "PROP-one" }] } }],
      currentUnderstanding: { body: "## Accepted direction\n\n- Build **the thing** for the accepted reason.\n- Keep `authority` explicit.\n\n<unsafe> stays text.", sourceObjectIds: ["INT-goal"] },
    }, "user-1");
    assert(direction.receiptMode === "direct_direction", "direct direction returns receipt mode");
    let malformedDirectionRejected = false;
    try { await domain.recordDirection(focus.id, { directions: [{ collection: "concepts", key: "bad-scope", value: { title: "Bad", body: "Bad scope.", scope: { kind: "workstreams", workstreamIds: 42 }, sourceRefs: [], relationships: [] } }] }, "user-bad"); }
    catch (error) { malformedDirectionRejected = String(error.message).startsWith("Invalid project model:"); }
    assert(malformedDirectionRejected, "malformed direct authority rejects cleanly before persistence");
    await domain.update(focus.id, {
      specViews: [{ id: "SPEC-root", kind: "spec", path: "spec/spec.md", title: "Demo", sections: [{ id: "purpose", title: "Purpose", objectIds: ["INT-goal"] }] }],
    });
    const preview = await domain.specs({ action: "preview" });
    const candidate = await readFile(join(preview.directory, "spec/spec.md"), "utf8");
    assertIncludes(candidate, "**Candidate:**", "candidate projection marks uncutover content");
    assertIncludes(candidate, "Build the thing.", "candidate projection includes model-owned prose");

    let duplicateReviewRejected = false;
    try { await domain.createReview(focus.id, { title: "Duplicate", points: [{ key: "same", title: "A", context: "A.", purpose: "awareness" }, { key: "same!", title: "B", context: "B.", purpose: "awareness" }] }); }
    catch { duplicateReviewRejected = true; }
    assert(duplicateReviewRejected, "normalized duplicate review point IDs are rejected");
    const review = await domain.createReview(focus.id, {
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

    const turn = await domain.reviewTurn(focus.id);
    assert(turn.review.points.length === 2 && turn.review.semanticHash === review.reviewHash, "live turn projection binds the exact multi-point review hash");
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
    const presented = await feedbackManager.present(turn, { noOpen: true, onUpdate: ({ phase }) => phases.push(phase), onPresented: async () => { assert(await domain.markReviewPresented(focus.id, turn.review.id, turn.review.semanticHash), "exact Lavish review is marked presented"); } });
    assert(presented.metadata.status === "feedback" && presented.feedback.prompts.length === 1, "production lifecycle returns bounded fake-CLI feedback");
    const feedbackRef = lavishFeedbackInteractionRef(turn.review.id, turn.review.semanticHash, presented.feedback);
    assert(feedbackRef.startsWith(`lavish-feedback:${turn.review.id}:sha256:`) && !feedbackRef.includes("Selected option"), "Lavish feedback binds a hash-only human interaction receipt");
    assert(phases.includes("rendered") && phases.includes("waiting") && phases.includes("feedback"), "production lifecycle reports long-running phases");
    assert(Boolean((await domain.sessions.load(focus.id)).activeReview?.presentedAt), "successful Lavish open records presentation before resolution");

    const waitManager = new ReviewPresentationManager(root, { cli: new LavishCliAdapter({ command: process.execPath, argsPrefix: [fakeLavish], env: { FAKE_MODE: "wait" } }) });
    const controller = new AbortController(); setTimeout(() => controller.abort(), 30);
    let aborted = false;
    try { await waitManager.resume(turn, { signal: controller.signal }); } catch (error) { aborted = error?.name === "AbortError"; }
    assert(aborted, "aborting a production Lavish poll propagates AbortError");
    assert((await waitManager.readMetadata(waitManager.paths(turn)))?.status === "interrupted", "aborted production poll remains resumable");
    const resumed = await feedbackManager.resume(turn);
    assert(resumed.metadata.status === "feedback", "production poll resumes the stable artifact");

    const endedManager = new ReviewPresentationManager(root, { cli: new LavishCliAdapter({ command: process.execPath, argsPrefix: [fakeLavish], env: { FAKE_MODE: "ended" } }) });
    const endedPresentation = await endedManager.resume(turn);
    assert(endedPresentation.metadata.status === "user_ended", "user-ended Lavish state is durable");
    let implicitReopenRejected = false;
    try { await endedManager.resume(turn); } catch (error) { implicitReopenRejected = String(error.message).includes("explicit reopen"); }
    assert(implicitReopenRejected, "user-ended Lavish sessions do not resume implicitly");
    let implicitPresentRejected = false;
    try { await feedbackManager.present(turn, { noOpen: true }); } catch (error) { implicitPresentRejected = String(error.message).includes("explicit reopen"); }
    assert(implicitPresentRejected, "user-ended Lavish sessions do not restart through present");
    await feedbackManager.cleanup(focus.id, turn.review.id);

    const compactWarnings = parsePollOutput(`session:\n  file: /fixture/review.html\n  status: feedback\nprompts[0]:\nlayout_warnings[1]{selector,kind,axis,overflowPx,viewportWidth,severity,persistent}:\n  \"#review\",content-overflow,horizontal,50,390,error,true\n`);
    assert(compactWarnings.layoutWarnings.length === 1 && compactWarnings.layoutWarnings[0].persistent, "production parser recognizes compact TOON warning tables");

    const malformedReviewSession = structuredClone(await domain.sessions.load(focus.id));
    malformedReviewSession.activeReview.points[0].options[0].direction.value.scope = { kind: "workstreams", workstreamIds: 42 };
    let malformedReviewRejected = false;
    try { validateFocusSession(malformedReviewSession); } catch (error) { malformedReviewRejected = String(error.message).startsWith("Invalid focus session:"); }
    assert(malformedReviewRejected, "persisted review authority payloads receive full runtime shape validation");
    await domain.markReviewPresented(focus.id, review.review.id);
    await domain.update(focus.id, { patch: [{ id: "PROP-two", changes: { body: "Changed after presentation." } }] });
    const resolved = await domain.resolveReview(focus.id, {
      outcomes: [
        { pointId: "point-one", action: "accept", optionId: "option-one" },
        { pointId: "point-two", action: "reject", optionId: "option-two" },
      ],
    }, "user-2");
    assert(resolved.appliedPointIds.includes("point-one"), "fresh sparse point resolves");
    assert(resolved.stalePointIds.includes("point-two"), "changed point remains stale");
    assert((await domain.sessions.load(focus.id)).activeReview?.points.length === 1, "stale point remains in a draft review");
    assert(resolved.remainingReview?.markdown.includes("Second"), "remaining review is re-presentable");
    await domain.markReviewPresented(focus.id, review.review.id);
    const beforeSessionFailure = await domain.models.load();
    let unresolvedRemovalRejected = false;
    try { await domain.resolveReview(focus.id, { outcomes: [], update: { removeIds: ["PROP-two"] } }, "user-remove"); }
    catch { unresolvedRemovalRejected = true; }
    assert(unresolvedRemovalRejected, "resolution cannot commit a model that invalidates its remaining review");
    assert((await domain.models.load()).project.revision === beforeSessionFailure.project.revision, "failed review finalization leaves model unchanged");
    assert(Boolean((await domain.sessions.load(focus.id)).activeReview), "failed review finalization preserves the review packet");
    const rejectedOutcome = await domain.resolveReview(focus.id, { outcomes: [{ pointId: "point-two", action: "reject" }] }, "user-reject");
    assert(rejectedOutcome.appliedPointIds.includes("point-two"), "reviewed rejection is applied durably");
    assert((await domain.models.load()).decisions.some(({ id }) => id === "DEC-reject-two"), "rejection creates its reviewed durable decision");

    await domain.update(focus.id, {
      add: [{ collection: "decisions", key: "refresh-target", value: { ...base("Refresh target", "Original candidate body."), rationale: "It is still being refined." } }],
      specViews: [{ id: "SPEC-root", kind: "spec", path: "spec/spec.md", title: "Demo", sections: [{ id: "purpose", title: "Purpose", objectIds: ["INT-goal", "DEC-one", "DEC-reject-two", "DEC-refresh-target"] }] }],
    });
    const refreshReview = await domain.createReview(focus.id, {
      id: "review-refresh-existing-direction",
      title: "Refresh an existing direction",
      points: [
        { id: "point-refresh-awareness", title: "Refresh awareness", context: "Resolving awareness refreshes the unresolved decision.", purpose: "awareness", objectIds: ["DEC-refresh-target"] },
        {
          id: "point-refresh-existing",
          title: "Accept the refreshed target",
          context: "The candidate may change before this point is resolved.",
          purpose: "decision",
          question: "Accept the current candidate?",
          objectIds: ["DEC-refresh-target"],
          options: [
            { id: "option-refresh-current", label: "Current", description: "Accept the candidate's current semantic payload.", objectId: "DEC-refresh-target", direction: { collection: "decisions", id: "DEC-refresh-target", state: "accepted" } },
            { id: "option-refresh-override", label: "Override", description: "Accept a deliberate body override while inheriting other current fields.", objectId: "DEC-refresh-target", direction: { collection: "decisions", id: "DEC-refresh-target", state: "accepted", value: { body: "Deliberate reviewed override." } } },
          ],
          rejectDirection: { collection: "decisions", id: "DEC-refresh-target", state: "rejected", value: { body: "Rejected reviewed override." } },
          deferDirection: { collection: "decisions", id: "DEC-refresh-target", state: "candidate", value: { body: "Deferred reviewed override." } },
        },
      ],
    });
    assert(!("directionValuePatch" in refreshReview.review.points[1].options[1]), "internal refresh metadata is excluded from the public review payload");
    await domain.markReviewPresented(focus.id, refreshReview.review.id);
    await domain.update(focus.id, { patch: [{ id: "DEC-refresh-target", changes: { title: "Refresh target revised", body: "Revised candidate body." } }] });
    const refreshedResolution = await domain.resolveReview(focus.id, { outcomes: [] }, "user-refresh-existing");
    assert(refreshedResolution.unresolvedPointIds.includes("point-refresh-existing"), "changed existing-target point remains unresolved for fresh review");
    const refreshedPoint = (await domain.sessions.load(focus.id)).activeReview.points[0];
    const currentDirection = refreshedPoint.options.find(({ id }) => id === "option-refresh-current").direction;
    const overrideDirection = refreshedPoint.options.find(({ id }) => id === "option-refresh-override").direction;
    assert(currentDirection.value.title === "Refresh target revised" && currentDirection.value.body === "Revised candidate body.", "refresh rematerializes an existing direction from the current target");
    assert(overrideDirection.value.title === "Refresh target revised" && overrideDirection.value.body === "Deliberate reviewed override.", "refresh reapplies an explicit direction patch over the current target");
    assert(refreshedPoint.rejectDirection.value.title === "Refresh target revised" && refreshedPoint.rejectDirection.value.body === "Rejected reviewed override.", "refresh reapplies an existing-target reject patch");
    assert(refreshedPoint.deferDirection.value.title === "Refresh target revised" && refreshedPoint.deferDirection.value.body === "Deferred reviewed override.", "refresh reapplies an existing-target defer patch");
    assert(refreshedPoint.options.find(({ id }) => id === "option-refresh-override").directionValuePatch.body === "Deliberate reviewed override.", "partial resolution preserves private refresh metadata in the focus session");
    await domain.sessions.mutate(focus.id, (session) => { delete session.activeReview.points[0].options.find(({ id }) => id === "option-refresh-current").directionValuePatch; });
    await domain.markReviewPresented(focus.id, refreshReview.review.id);
    await domain.update(focus.id, { patch: [{ id: "DEC-refresh-target", changes: { rationale: "The latest rationale." } }] });
    await domain.resolveReview(focus.id, { outcomes: [] }, "user-refresh-existing-again");
    const refreshedReviewAgain = (await domain.sessions.load(focus.id)).activeReview.points[0];
    const refreshedAgain = refreshedReviewAgain.options.find(({ id }) => id === "option-refresh-override");
    const legacyCurrentAgain = refreshedReviewAgain.options.find(({ id }) => id === "option-refresh-current");
    assert(refreshedAgain.direction.value.rationale === "The latest rationale." && refreshedAgain.direction.value.body === "Deliberate reviewed override.", "repeated refresh preserves the explicit patch while inheriting later target changes");
    assert(legacyCurrentAgain.direction.value.rationale === "The latest rationale." && legacyCurrentAgain.direction.value.body === "Revised candidate body.", "legacy sessions without private patch metadata refresh from the current target");
    await domain.markReviewPresented(focus.id, refreshReview.review.id);
    const acceptedRefresh = await domain.resolveReview(focus.id, { outcomes: [{ pointId: "point-refresh-existing", action: "accept", optionId: "option-refresh-current" }] }, "user-accept-refresh");
    assert(acceptedRefresh.appliedPointIds.includes("point-refresh-existing"), "refreshed exact direction resolves successfully");
    const acceptedRefreshTarget = (await domain.models.load()).decisions.find(({ id }) => id === "DEC-refresh-target");
    assert(acceptedRefreshTarget.state === "accepted" && acceptedRefreshTarget.body === "Revised candidate body.", "accepted refreshed direction commits the current reviewed payload");

    const directReview = await domain.createReview(focus.id, {
      id: "review-direct-reconciliation",
      title: "Direct reconciliation",
      points: [
        {
          id: "point-direct-one",
          title: "First direct choice",
          context: "This can be answered directly.",
          purpose: "decision",
          question: "Choose the first direction?",
          options: [{ id: "option-direct-one", label: "First", description: "Accept the first direction.", direction: { collection: "decisions", newId: "DEC-direct-review-one", value: { title: "Direct review one", body: "The first direct review direction.", scope: { kind: "repository" }, sourceRefs: [], relationships: [], rationale: "It is exact." } } }],
        },
        {
          id: "point-direct-two",
          title: "Second direct choice",
          context: "This remains independently unresolved.",
          purpose: "decision",
          question: "Choose the second direction?",
          options: [{ id: "option-direct-two", label: "Second", description: "Accept the second direction.", direction: { collection: "decisions", newId: "DEC-direct-review-two", value: { title: "Direct review two", body: "The second direct review direction.", scope: { kind: "repository" }, sourceRefs: [], relationships: [], rationale: "It is also exact." } } }],
        },
      ],
    });
    const persistedDirectReview = structuredClone((await domain.sessions.load(focus.id)).activeReview);
    const firstDirect = await domain.recordDirection(focus.id, {
      directions: [directReview.review.points[0].options[0].direction],
      specViews: [{ id: "SPEC-root", kind: "spec", path: "spec/spec.md", title: "Demo", sections: [{ id: "purpose", title: "Purpose", objectIds: ["INT-goal", "DEC-one", "DEC-reject-two", "DEC-refresh-target", "DEC-direct-review-one"] }] }],
    }, "user-direct-one");
    assert(firstDirect.reconciledReviewPointIds.includes("point-direct-one"), "equivalent direct authority reconciles an unpresented active review point");
    const remainingDirectReview = (await domain.sessions.load(focus.id)).activeReview;
    assert(remainingDirectReview?.points.length === 1 && remainingDirectReview.points[0].id === "point-direct-two" && !remainingDirectReview.presentedAt, "direct reconciliation preserves unrelated points as an unpresented continuation");
    const secondDirect = await domain.recordDirection(focus.id, {
      directions: [remainingDirectReview.points[0].options[0].direction],
      specViews: [{ id: "SPEC-root", kind: "spec", path: "spec/spec.md", title: "Demo", sections: [{ id: "purpose", title: "Purpose", objectIds: ["INT-goal", "DEC-one", "DEC-reject-two", "DEC-refresh-target", "DEC-direct-review-one", "DEC-direct-review-two"] }] }],
    }, "user-direct-two");
    assert(secondDirect.reconciledReviewPointIds.includes("point-direct-two") && !(await domain.sessions.load(focus.id)).activeReview, "equivalent direct authority clears the final stale review point");
    await domain.sessions.mutate(focus.id, (session) => { session.activeReview = persistedDirectReview; });
    const recoveredReview = await domain.reconcileSatisfiedReview(focus.id);
    assert(recoveredReview.reconciledReviewPointIds.length === 2 && !(await domain.sessions.load(focus.id)).activeReview, "startup reconciliation clears a preexisting review whose exact directions are already accepted");

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
    const cutover = await domain.cutover(focus.id, candidateManifestHash(model), "user-cutover");
    assert(cutover.receiptMode === "migration_cutover", "cutover records a migration authority receipt");
    assertIncludes(await readFile(join(root, "spec/spec.md"), "utf8"), "generated-by: pi-dag-workflow/project-model", "audited cutover replaces declared hand-maintained projection targets");

    await domain.recordDirection(focus.id, {
      directions: [{
        collection: "decisions",
        key: "one-v2",
        value: {
          ...base("Choose one v2", "One is replaced by the current direction."),
          rationale: "The newer direction supersedes the older accepted decision.",
          relationships: [{ kind: "supersedes", targetId: "DEC-one" }],
        },
      }],
      specViews: [{ id: "SPEC-root", kind: "spec", path: "spec/spec.md", title: "Demo", sections: [{ id: "purpose", title: "Purpose", objectIds: ["INT-goal", "DEC-one-v2", "DEC-reject-two", "DEC-refresh-target", "DEC-direct-review-one", "DEC-direct-review-two"] }] }],
    }, "user-supersede");
    const supersededProjection = await readFile(join(root, "spec/spec.md"), "utf8");
    assertIncludes(supersededProjection, "One is replaced by the current direction.", "accepted superseder renders as governing direction");
    assert(!supersededProjection.includes("One is selected."), "superseded accepted decision stops rendering");
    const governing = await domain.context(focus.id, { view: "governing" });
    assert(!governing.some(({ id }) => id === "DEC-one") && governing.some(({ id }) => id === "DEC-one-v2"), "governing context excludes superseded accepted decisions");
    assert(validateProjectModel(await domain.models.load()).length === 0, "superseded accepted objects no longer require canonical generated-spec placement");

    await domain.update(focus.id, {
      add: [{ collection: "questions", key: "reconsider", value: { ...base("Reconsider goal", "Should the goal change?"), kind: "reconsideration", relationships: [{ kind: "challenges", targetId: "INT-goal" }] } }],
    });
    const generated = await readFile(join(root, "spec/spec.md"), "utf8");
    assertIncludes(generated, "**Under review:**", "non-authoritative reconsideration synchronizes current spec marker");
    const check = await domain.specs({ action: "check" });
    assert(check.ok, "generated specs check cleanly");

    const beforeCollision = await domain.models.load();
    await writeFile(join(root, "spec/spec.md"), "# Human file\n");
    let collisionRejected = false;
    try { await domain.update(focus.id, { add: [{ collection: "discoveries", key: "collision", value: base("Collision", "Must roll back.") }] }); }
    catch { collisionRejected = true; }
    assert(collisionRejected, "unmanaged generated-spec collision rejects the semantic transaction");
    assert((await domain.models.load()).project.revision === beforeCollision.project.revision, "projection failure leaves model authority unchanged");
    assert((await readFile(join(root, "spec/spec.md"), "utf8")) === "# Human file\n", "projection rollback preserves collided human content");
    await domain.specs({ action: "generate", replaceUnmanaged: true });

    let rejected = false;
    try { await domain.update(focus.id, { patch: [{ id: "INT-goal", changes: { body: "Unauthorized rewrite." } }] }); }
    catch { rejected = true; }
    assert(rejected, "ordinary update cannot rewrite accepted semantics");
    assert(validateProjectModel(await domain.models.load()).length === 0, "stored authoritative model remains valid");

    const beforeRejectedDirection = await domain.models.load();
    let unroutedRejected = false;
    try { await domain.recordDirection(focus.id, { directions: [{ collection: "commitments", key: "unrouted", value: base("Unrouted", "Must not partially commit.") }] }, "user-3"); }
    catch { unroutedRejected = true; }
    assert(unroutedRejected, "authoritative direction without canonical placement is rejected");
    assert((await domain.models.load()).project.revision === beforeRejectedDirection.project.revision, "failed authority mutation leaves model revision unchanged");

    let forged = false;
    try {
      await domain.update(focus.id, { add: [{ collection: "intents", key: "forged", value: { ...base("Forged", "Not human authority."), kind: "outcome", state: "accepted", acceptance: { actor: "user", mode: "direct_direction", acceptedAt: new Date().toISOString(), interactionRef: "fake", contentHash: "sha256:" + "0".repeat(64) } } }] });
    } catch { forged = true; }
    assert(forged, "ordinary update cannot forge acceptance fields or governing state");
  });
}

async function testMigration() {
  await withTemp("migration", async (root) => {
    await mkdir(join(root, "spec"), { recursive: true });
    await writeFile(join(root, "spec/spec.md"), "# Legacy\n");
    const legacy = {
      id: "legacy", title: "Legacy", neighborhoods: [{ id: "area", title: "Area", status: "active" }],
      tangents: [], questions: [], evidence: [], proposals: [], probes: [], promotions: [],
      decisions: [{ id: "D-old", title: "Keep behavior", contract: "Behavior remains.", rationale: "Needed.", status: "active", questionIds: [] }],
    };
    const migrated = await migrateLegacyBrainstorm(root, legacy);
    assert(migrated.model.project.mode === "candidate", "migration creates candidate mode");
    assert(migrated.model.commitments.length === 1, "legacy behavior maps to candidate commitment");
    assertIncludes(migrated.report, "pending semantic audit", "migration report requires audit");
    assert(validateProjectModel(migrated.model).length === 0, "migration candidate validates");
  });
}

async function testPiIntegration() {
  await withTemp("pi", async (root) => {
    const domain = new ProjectModelDomain(root);
    await domain.models.initialize("demo", "Demo");
    const pi = new FakePi();
    dagWorkflow(pi);
    pi.loading = false;
    const ctx = pi.context(root);
    await pi.runCommand("dag", "brainstorm new Schema focus", ctx);
    assert(pi.activeTools.has("dag_model_context"), "brainstorm command activates model tools");
    assert(["subagent", "subagent_status", "subagent_inspect", "subagent_tail", "subagent_cancel", "subagent_retry"].every((name) => pi.tools.has(name)), "generic owned-worker tools are registered independently of DAG mode");
    assert(pi.commands.has("workers"), "generic /workers command is registered");
    assert(!pi.tools.has("dag_init") && !pi.tools.has("dag_start_node"), "mutating legacy DAG tools are not registered");
    assert(![...pi.tools].some(([name]) => name.startsWith("dag_grillme")), "GrillMe tools are absent");
    let seedAuthorityRejected = false;
    try { await pi.callTool("dag_model_record_direction", { directions: [{ collection: "intents", key: "seed", value: { ...base("Seed", "Must be classified first."), kind: "outcome" } }] }, ctx); }
    catch { seedAuthorityRejected = true; }
    assert(seedAuthorityRejected, "brainstorm command seed is not an authority receipt");

    await pi.emit("input", { source: "interactive", text: "Use this goal" }, ctx);
    await pi.callTool("dag_model_record_direction", {
      directions: [{ collection: "intents", key: "goal", value: { ...base("Goal", "Use this goal."), kind: "outcome" } }],
    }, ctx);
    assert((await domain.models.load()).intents.length === 1, "Pi direct-direction adapter writes accepted intent");
    await pi.callTool("dag_model_update", { specViews: [{ id: "SPEC-root", kind: "spec", path: "spec/spec.md", title: "Demo", sections: [{ id: "purpose", title: "Purpose", objectIds: ["INT-goal"] }] }] }, ctx);
    const candidate = await domain.models.load();
    await pi.emit("input", { source: "interactive", text: "Accept this exact candidate hash" }, ctx);
    await pi.callTool("dag_model_record_direction", { cutover: { candidateManifestHash: candidateManifestHash(candidate) } }, ctx);
    assert((await domain.models.load()).project.mode === "authoritative", "Pi cutover path binds and activates the audited candidate");
    const reviewTool = await pi.callTool("dag_model_review", { title: "Awareness", points: [{ key: "notice", title: "Notice", context: "Exact packet.", purpose: "awareness" }] }, ctx);
    assert(reviewTool.details.presented === true, "visible review tool result reports exact presentation");
    assert((await domain.sessions.load("focus-schema-focus")).activeReview.presentedAt, "visible exact review tool result enables resolution without manual marker recovery");
    assert(pi.activeTools.has("dag_model_present_review"), "brainstorm mode activates the optional presentation adapter tool");

    await pi.emit("session_start", { reason: "fork" }, ctx);
    assert(pi.activeTools.has("dag_model_context"), "fork restores exact linked focus automatically");
    await withTemp("other-repository", async (otherRoot) => {
      const otherCtx = pi.context(otherRoot);
      await pi.emit("session_start", { reason: "switch-repository" }, otherCtx);
      assert(!pi.activeTools.has("dag_model_context"), "focus link cannot activate in another repository");
    });
    await pi.runCommand("dag", "brainstorm resume focus-schema-focus", ctx);
    await pi.runCommand("dag", "plan", ctx);
    assert(ctx.ui.notifications.some(({ message }) => message.includes("deferred")), "legacy plan command is disabled clearly");
    await pi.runCommand("dag", "brainstorm stop", ctx);
    await pi.emit("session_start", { reason: "reload" }, ctx);
    assert(!pi.activeTools.has("dag_model_context") && !pi.activeTools.has("dag_model_present_review"), "suspended focus is not restored by an older active session link");
  });
}

function base(title, body) {
  return { title, body, scope: { kind: "repository" }, introducedBy: "agent", sourceRefs: [], relationships: [] };
}

class FakePi {
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
  async emit(event, payload, ctx) { for (const handler of this.handlers.get(event) ?? []) await handler(payload, ctx); }
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

async function withTemp(name, fn) {
  const root = await mkdtemp(join(tmpdir(), `pi-model-${name}-`));
  try { await fn(root); } finally { await rm(root, { recursive: true, force: true }); }
}
function assertIncludes(text, value, message) { assert(text.includes(value), `${message}: missing ${value}`); }
function assert(value, message) { if (!value) throw new Error(`Project model test failed: ${message}`); }

await testDomainAndProjection();
await testMigration();
await testPiIntegration();
console.log("Project model production tests OK");
