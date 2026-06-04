import { CustomEditor, rawKeyHint, type ExtensionContext, type KeybindingsManager } from "@earendil-works/pi-coding-agent";
import type { EditorTheme, TUI } from "@earendil-works/pi-tui";
import { Key, matchesKey, truncateToWidth, visibleWidth, wrapTextWithAnsi } from "@earendil-works/pi-tui";
import { currentQuestion, getActiveGrillMe, saveGrillMe, type GrillMeSession } from "./state.ts";

let activeTui: TUI | undefined;
export function requestGrillMeRender() { activeTui?.requestRender(); }

function stripAnsi(s: string): string { return s.replace(/\x1b\[[0-9;]*m/g, ""); }
function pushWrapped(lines: string[], text: string, width: number, indent = "") {
  for (const line of wrapTextWithAnsi(text, Math.max(8, width - visibleWidth(indent)))) lines.push(truncateToWidth(indent + line, width));
}
function nextIndex(session: GrillMeSession, direction: 1 | -1): number {
  if (session.questions.length === 0) return 0;
  let i = session.currentIndex;
  for (let seen = 0; seen < session.questions.length; seen++) {
    i = (i + direction + session.questions.length) % session.questions.length;
    if (session.questions[i]?.status !== "discarded") return i;
  }
  return session.currentIndex;
}
function modeLabel(session: GrillMeSession): string {
  if (session.mode === "nav") return "NAV: h prev · l next · j/k option · Enter answer · a freeform · c chat";
  if (session.mode === "answer") return "ANSWER: Enter saves freeform · Esc nav";
  return "CHAT: Enter asks top-level agent · Esc nav";
}

export class GrillMeEditor extends CustomEditor {
  constructor(
    tui: TUI,
    theme: EditorTheme,
    keybindings: KeybindingsManager,
    private ctx: ExtensionContext,
    private onComplete: (session: GrillMeSession) => void,
  ) {
    super(tui, theme, keybindings, { paddingX: 0 });
    activeTui = tui;
  }

  private session(): GrillMeSession | undefined { return getActiveGrillMe(); }
  private refresh() { void saveGrillMe(this.ctx); activeTui?.requestRender(); }

  private answerWithOption(index: number) {
    const session = this.session();
    const q = currentQuestion(session);
    const option = q?.options?.[index];
    if (!session || !q || !option || q.status === "discarded") return;
    q.answer = option.text;
    q.status = "answered";
    q.answerMode = "option";
    q.updatedAt = new Date().toISOString();
    session.currentIndex = nextIndex(session, 1);
    session.selectedOptionIndex = 0;
    session.mode = "nav";
    this.setText("");
    this.refresh();
  }

  private saveAnswer() {
    const session = this.session();
    const q = currentQuestion(session);
    if (!session || !q || q.status === "discarded") return;
    const answer = this.getExpandedText().trim();
    if (!answer) { this.ctx.ui.notify("Write an answer first.", "warning"); return; }
    q.answer = answer;
    q.status = "answered";
    q.answerMode = "freeform";
    q.updatedAt = new Date().toISOString();
    session.currentIndex = nextIndex(session, 1);
    session.selectedOptionIndex = 0;
    session.mode = "nav";
    this.setText("");
    this.refresh();
  }

  private discard() {
    const session = this.session();
    const q = currentQuestion(session);
    if (!session || !q) return;
    q.status = "discarded";
    q.updatedAt = new Date().toISOString();
    session.currentIndex = nextIndex(session, 1);
    session.selectedOptionIndex = 0;
    session.mode = "nav";
    this.setText("");
    this.refresh();
  }

  private complete() {
    const session = this.session();
    if (!session) return;
    if (session.completedAt) {
      this.ctx.ui.setEditorComponent(undefined);
      activeTui?.requestRender();
      this.ctx.ui.notify(`GrillMe ${session.fileNumber} is already complete; closed GrillMe UI.`, "info");
      return;
    }
    const timestamp = new Date().toISOString();
    for (const q of session.questions) {
      if (q.status === "unanswered") {
        q.status = "discarded";
        q.updatedAt = timestamp;
      }
    }
    session.completedAt = timestamp;
    session.mode = "nav";
    this.setText("");
    void saveGrillMe(this.ctx, session).then(() => {
      this.ctx.ui.setEditorComponent(undefined);
      activeTui?.requestRender();
      this.ctx.ui.notify(`GrillMe ${session.fileNumber} complete and closed. Asking the agent to synthesize answers into .ai/project.md.`, "info");
      this.onComplete(session);
    });
  }

  handleInput(data: string): void {
    const session = this.session();
    if (!session) { super.handleInput(data); return; }
    if (matchesKey(data, Key.escape)) { session.mode = "nav"; this.refresh(); return; }

    if (session.mode === "nav") {
      if (data === "h" || matchesKey(data, Key.left)) { session.currentIndex = nextIndex(session, -1); session.selectedOptionIndex = 0; this.setText(""); this.refresh(); return; }
      if (data === "l" || matchesKey(data, Key.right)) { session.currentIndex = nextIndex(session, 1); session.selectedOptionIndex = 0; this.setText(""); this.refresh(); return; }
      if (data === "j" || matchesKey(data, Key.down)) { const n = currentQuestion(session)?.options?.length ?? 0; if (n) session.selectedOptionIndex = (session.selectedOptionIndex + 1) % n; this.refresh(); return; }
      if (data === "k" || matchesKey(data, Key.up)) { const n = currentQuestion(session)?.options?.length ?? 0; if (n) session.selectedOptionIndex = (session.selectedOptionIndex - 1 + n) % n; this.refresh(); return; }
      if (this.keybindings.matches(data, "tui.input.submit")) { const n = currentQuestion(session)?.options?.length ?? 0; if (n) this.answerWithOption(session.selectedOptionIndex); else { session.mode = "answer"; this.refresh(); } return; }
      if (data === "a") { session.mode = "answer"; this.setText(currentQuestion(session)?.answer ?? ""); this.refresh(); return; }
      if (data === "c") { session.mode = "chat"; this.setText(""); this.refresh(); return; }
      if (data === "d") { this.discard(); return; }
      if (data === "x") { this.complete(); return; }
      if (data === "e") { session.mode = "answer"; this.setText(currentQuestion(session)?.answer ?? ""); this.refresh(); return; }
      if (data.length === 1 && data.charCodeAt(0) >= 32) return;
      super.handleInput(data); return;
    }

    if (session.mode === "chat") { super.handleInput(data); return; }
    if (this.keybindings.matches(data, "tui.input.submit")) { this.saveAnswer(); return; }
    super.handleInput(data);
  }

  render(width: number): string[] {
    const session = this.session();
    if (!session) return super.render(width);
    const theme = this.ctx.ui.theme;
    const q = currentQuestion(session);
    const lines: string[] = [];
    const add = (s: string) => lines.push(truncateToWidth(s, width));
    const counts = {
      answered: session.questions.filter((item) => item.status === "answered").length,
      unanswered: session.questions.filter((item) => item.status === "unanswered").length,
      discarded: session.questions.filter((item) => item.status === "discarded").length,
    };
    const rule = theme.fg("borderAccent", "─".repeat(Math.max(1, width)));
    add(rule);
    add(theme.fg("accent", ` GrillMe ${session.fileNumber} `) + theme.fg("muted", `Q ${session.currentIndex + 1}/${session.questions.length} · ${session.completedAt ? "COMPLETE" : modeLabel(session)} · ✓ ${counts.answered} · ? ${counts.unanswered} · ✕ ${counts.discarded}`));
    add(rule);
    if (session.completedAt) {
      add(theme.fg("success", ` GrillMe completed at ${session.completedAt}.`));
      add(theme.fg("muted", " The agent has been asked to read filtered answers with dag_grillme_get_answers and synthesize them into .ai/project.md."));
    } else if (!q) add(theme.fg("warning", " No questions loaded. Ask the agent to call dag_grillme_set_questions."));
    else {
      const statusColor = q.status === "answered" ? "success" : q.status === "discarded" ? "warning" : "muted";
      add(theme.fg("accent", ` ${q.title}`) + theme.fg(statusColor, ` [${q.status}]`));
      pushWrapped(lines, theme.fg("text", q.body), width, " ");
      if (q.why) pushWrapped(lines, theme.fg("muted", `Why this matters: ${q.why}`), width, " ");
      if (q.options?.length) {
        lines.push("");
        q.options.forEach((option, index) => {
          const selected = session.mode === "nav" && index === session.selectedOptionIndex;
          const prefix = selected ? theme.fg("accent", " > ") : "   ";
          const label = theme.fg(selected ? "accent" : "muted", `${index + 1}. ${option.label}: `);
          const text = selected ? theme.bg("selectedBg", theme.fg("text", option.text)) : theme.fg("text", option.text);
          pushWrapped(lines, prefix + label + text, width, " ");
        });
      }
      if (q.answer) { lines.push(""); pushWrapped(lines, theme.fg("success", "Saved answer: ") + theme.fg("muted", q.answer), width, " "); }
    }
    lines.push("");
    const buttons = session.mode === "nav"
      ? [rawKeyHint("h", "Prev"), rawKeyHint("l", "Next"), rawKeyHint("j/k", "Option"), rawKeyHint("Enter", "Answer selected"), rawKeyHint("a", "Freeform"), rawKeyHint("c", "Chat"), rawKeyHint("d", "Discard"), rawKeyHint("x", "Complete")]
      : session.mode === "answer"
        ? [rawKeyHint("Enter", "Save"), rawKeyHint("Shift+Enter", "Newline"), rawKeyHint("Esc", "Nav")]
        : [rawKeyHint("Enter", "Ask agent"), rawKeyHint("Esc", "Nav")];
    const buttonText = buttons.map(stripAnsi);
    const footer = session.mode === "nav"
      ? `${theme.fg("dim", buttonText.slice(0, -1).join("  "))}  ${theme.fg("text", buttonText.at(-1) ?? "")}`
      : theme.fg("dim", buttonText.join("  "));
    pushWrapped(lines, footer, width, " ");
    add(rule);
    return [...lines, ...super.render(width)];
  }
}

export function installGrillMeEditor(ctx: ExtensionContext, onComplete: (session: GrillMeSession) => void) {
  ctx.ui.setEditorComponent((tui, theme, keybindings) => new GrillMeEditor(tui, theme, keybindings, ctx, onComplete));
}
