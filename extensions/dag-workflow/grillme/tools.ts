import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { appendProjectUnderstanding, currentQuestion, getActiveGrillMe, grillMeAnswers, loadGrillMe, loadLatestGrillMe, saveGrillMe, setActiveGrillMe, type GrillMeQuestion, type GrillMeSession } from "./state.ts";
import { requestGrillMeRender } from "./editor.ts";

const OptionSchema = Type.Object({ id: Type.String(), label: Type.String(), text: Type.String() });
const QuestionSchema = Type.Object({
  id: Type.String(),
  title: Type.String(),
  body: Type.String(),
  why: Type.Optional(Type.String()),
  options: Type.Optional(Type.Array(OptionSchema)),
});

function normalizeQuestion(raw: any): GrillMeQuestion {
  return { ...raw, status: raw.status ?? "unanswered", updatedAt: new Date().toISOString() };
}

const reopenedCompletedSessions = new WeakSet<GrillMeSession>();

export function allowCompletedGrillMeMutation(session: GrillMeSession) {
  reopenedCompletedSessions.add(session);
}

function assertWritableSession(session: GrillMeSession) {
  if (session.completedAt && !reopenedCompletedSessions.has(session)) {
    throw new Error(`GrillMe ${session.fileNumber} is complete. Run /dag grillme to create a new GrillMe, or explicitly reopen this one before changing its questions.`);
  }
}

export function registerGrillMeTools(pi: ExtensionAPI, onQuestionsSaved?: (ctx: ExtensionContext, session: GrillMeSession) => void) {
  pi.registerTool({
    name: "dag_grillme_set_questions",
    label: "Set GrillMe Questions",
    description: "Create or replace the active GrillMe question queue with up to 100 questions.",
    parameters: Type.Object({ questions: Type.Array(QuestionSchema) }),
    async execute(_id, params, _signal, _onUpdate, ctx) {
      const session = getActiveGrillMe();
      if (!session) {
        throw new Error("No active GrillMe session. Run /dag grillme before calling dag_grillme_set_questions.");
      }
      assertWritableSession(session);
      const questions = params.questions.slice(0, 100).map(normalizeQuestion);
      session.questions = questions;
      session.currentIndex = 0;
      session.selectedOptionIndex = 0;
      session.mode = "nav";
      if (session.completedAt) delete session.completedAt;
      setActiveGrillMe(session);
      await saveGrillMe(ctx, session);
      onQuestionsSaved?.(ctx, session);
      requestGrillMeRender();
      return { content: [{ type: "text", text: `Loaded ${questions.length} GrillMe questions.` }], details: { count: questions.length } };
    },
  });

  pi.registerTool({
    name: "dag_grillme_update_questions",
    label: "Update GrillMe Questions",
    description: "Replace unanswered GrillMe questions. Answered questions are preserved unless includeAnswered is true.",
    parameters: Type.Object({ questions: Type.Array(QuestionSchema), includeAnswered: Type.Optional(Type.Boolean()) }),
    async execute(_id, params, _signal, _onUpdate, ctx) {
      const session = getActiveGrillMe();
      if (!session) {
        throw new Error("No active GrillMe session. Run /dag grillme before calling dag_grillme_update_questions.");
      }
      assertWritableSession(session);
      const answered = session.questions.filter((q) => q.status === "answered" && !params.includeAnswered);
      session.questions = [...answered, ...params.questions.slice(0, 100 - answered.length).map(normalizeQuestion)];
      session.currentIndex = 0;
      session.selectedOptionIndex = 0;
      session.mode = "nav";
      if (session.completedAt) delete session.completedAt;
      setActiveGrillMe(session);
      await saveGrillMe(ctx, session);
      onQuestionsSaved?.(ctx, session);
      requestGrillMeRender();
      return { content: [{ type: "text", text: `Updated GrillMe questions (${session.questions.length} total).` }], details: { count: session.questions.length } };
    },
  });

  pi.registerTool({
    name: "dag_grillme_get_answers",
    label: "Get GrillMe Answers",
    description: "Read GrillMe JSON state from disk and return answered questions only, filtered to id/title/body/answer and excluding discarded questions.",
    parameters: Type.Object({ fileNumber: Type.Optional(Type.Number()) }),
    async execute(_id, params, _signal, _onUpdate, ctx) {
      const active = getActiveGrillMe();
      const session = params.fileNumber !== undefined
        ? await loadGrillMe(ctx.cwd, params.fileNumber)
        : active
          ? await loadGrillMe(ctx.cwd, active.fileNumber)
          : await loadLatestGrillMe(ctx.cwd);
      if (!session) {
        throw new Error("No GrillMe session found.");
      }
      const answers = grillMeAnswers(session);
      return { content: [{ type: "text", text: JSON.stringify(answers, null, 2) }], details: { answers, sessionId: session.id, fileNumber: session.fileNumber } };
    },
  });

  pi.registerTool({
    name: "dag_grillme_get_state",
    label: "Get GrillMe State",
    description: "Return compact active GrillMe state.",
    parameters: Type.Object({}),
    async execute() {
      const session = getActiveGrillMe();
      const q = currentQuestion(session);
      return { content: [{ type: "text", text: JSON.stringify({ active: !!session, currentQuestion: q, count: session?.questions.length ?? 0 }, null, 2) }], details: session ?? {} };
    },
  });

  pi.registerTool({
    name: "dag_grillme_record_understanding",
    label: "Record GrillMe Understanding",
    description: "Append/update .ai/project.md with current understanding, research summary, links, decisions, uncertainty, or conflicts.",
    parameters: Type.Object({ markdown: Type.String() }),
    async execute(_id, params, _signal, _onUpdate, ctx) {
      await appendProjectUnderstanding(ctx, params.markdown);
      return { content: [{ type: "text", text: "Updated .ai/project.md with GrillMe understanding." }], details: {} };
    },
  });
}
