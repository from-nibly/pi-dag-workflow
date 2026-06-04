import { mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { join } from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";

export type GrillMeMode = "nav" | "answer" | "chat";
export type GrillMeQuestionStatus = "unanswered" | "answered" | "discarded";

export interface GrillMeOption { id: string; label: string; text: string }
export interface GrillMeQuestion {
  id: string;
  title: string;
  body: string;
  why?: string;
  options?: GrillMeOption[];
  status: GrillMeQuestionStatus;
  answer?: string;
  answerMode?: "option" | "freeform";
  updatedAt: string;
}
export interface GrillMeSession {
  id: string;
  fileNumber: number;
  createdAt: string;
  updatedAt: string;
  currentIndex: number;
  selectedOptionIndex: number;
  mode: GrillMeMode;
  questions: GrillMeQuestion[];
}

let active: GrillMeSession | undefined;

export function getActiveGrillMe(): GrillMeSession | undefined { return active; }
export function setActiveGrillMe(session: GrillMeSession | undefined) { active = session; }

export function grillmeDir(cwd: string): string { return join(cwd, ".ai", "grillme"); }
export function jsonPath(cwd: string, n: number): string { return join(grillmeDir(cwd), `grillme-${n}.json`); }
export function mdPath(cwd: string, n: number): string { return join(grillmeDir(cwd), `grillme-${n}.md`); }

export async function nextGrillMeNumber(cwd: string): Promise<number> {
  try {
    const files = await readdir(grillmeDir(cwd));
    const nums = files.map((f) => f.match(/^grillme-(\d+)\.json$/)?.[1]).filter(Boolean).map(Number);
    return (nums.length ? Math.max(...nums) : 0) + 1;
  } catch { return 1; }
}

export async function loadLatestGrillMe(cwd: string): Promise<GrillMeSession | undefined> {
  try {
    const files = await readdir(grillmeDir(cwd));
    const nums = files.map((f) => f.match(/^grillme-(\d+)\.json$/)?.[1]).filter(Boolean).map(Number).sort((a,b)=>a-b);
    const n = nums.at(-1);
    if (!n) return undefined;
    return JSON.parse(await readFile(jsonPath(cwd, n), "utf8")) as GrillMeSession;
  } catch { return undefined; }
}

export async function saveGrillMe(ctx: ExtensionContext, session = active): Promise<void> {
  if (!session) return;
  session.updatedAt = new Date().toISOString();
  await mkdir(grillmeDir(ctx.cwd), { recursive: true });
  await writeFile(jsonPath(ctx.cwd, session.fileNumber), `${JSON.stringify(session, null, 2)}\n`, "utf8");
  await writeFile(mdPath(ctx.cwd, session.fileNumber), renderGrillMeMarkdown(session), "utf8");
}

export async function createGrillMe(cwd: string, questions: GrillMeQuestion[] = []): Promise<GrillMeSession> {
  const n = await nextGrillMeNumber(cwd);
  return {
    id: `grillme-${n}`,
    fileNumber: n,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    currentIndex: 0,
    selectedOptionIndex: 0,
    mode: "nav",
    questions,
  };
}

export function currentQuestion(session = active): GrillMeQuestion | undefined {
  if (!session) return undefined;
  if (session.questions.length === 0) return undefined;
  session.currentIndex = Math.max(0, Math.min(session.currentIndex, session.questions.length - 1));
  const optionCount = session.questions[session.currentIndex]?.options?.length ?? 0;
  session.selectedOptionIndex = optionCount === 0 ? 0 : Math.max(0, Math.min(session.selectedOptionIndex, optionCount - 1));
  return session.questions[session.currentIndex];
}

export function renderGrillMeMarkdown(session: GrillMeSession): string {
  const lines = [`# GrillMe ${session.fileNumber}`, ""];
  for (const [index, q] of session.questions.entries()) {
    lines.push(`## Q${index + 1} - ${q.title}`, "", `Status: ${q.status}`, "", q.body, "");
    if (q.why) lines.push(`Why this matters: ${q.why}`, "");
    for (const option of q.options ?? []) lines.push(`${option.id.toUpperCase()}. ${option.label} — ${option.text}`);
    if (q.options?.length) lines.push("");
    lines.push(`> ${q.answer ?? ""}`, "");
  }
  return lines.join("\n");
}

export async function appendProjectUnderstanding(ctx: ExtensionContext, text: string): Promise<void> {
  const path = join(ctx.cwd, ".ai", "project.md");
  const existing = existsSync(path) ? await readFile(path, "utf8") : "# Project Brief\n";
  await mkdir(join(ctx.cwd, ".ai"), { recursive: true });
  await writeFile(path, `${existing.trim()}\n\n## GrillMe understanding update\n\n${text.trim()}\n`, "utf8");
}
