import { spawn } from "node:child_process";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";

export interface FeedbackLimits { maxPrompts: number; maxPromptBytes: number; maxTotalBytes: number; maxWarnings: number }
export interface LavishSession { file: string; status: string; sessionEnded?: boolean; endedBy?: string }
export interface LavishPrompt { uid: string; prompt: string; selector: string; tag: string; text: string; target?: Record<string, unknown> }
export interface LavishLayoutWarning { selector: string; kind: string; axis?: string; overflowPx: number; viewportWidth: number; severity: string; persistent: boolean }
export interface LavishFeedback {
  session: LavishSession;
  prompts: LavishPrompt[];
  layoutWarnings: LavishLayoutWarning[];
  truncation: { truncated: boolean; originalPromptCount: number; returnedPromptCount: number; originalBytes: number; returnedBytes: number; droppedPrompts: number; droppedBytes: number; droppedWarnings: number };
}
export interface ProcessResult { code: number; signal: NodeJS.Signals | null; stdout: string; stderr: string }

export const DEFAULT_FEEDBACK_LIMITS: Readonly<FeedbackLimits> = Object.freeze({ maxPrompts: 20, maxPromptBytes: 4_000, maxTotalBytes: 16_000, maxWarnings: 20 });

export class LavishCliAdapter {
  readonly command?: string;
  readonly argsPrefix: string[];
  readonly dedicatedOpenCommand: string;
  readonly dedicatedOpenArgsPrefix: string[];
  readonly cwd: string;
  readonly env: NodeJS.ProcessEnv;
  readonly maxRawBytes: number;
  readonly limits: FeedbackLimits;

  constructor(input: { command?: string; argsPrefix?: string[]; dedicatedOpenCommand?: string; dedicatedOpenArgsPrefix?: string[]; cwd?: string; env?: NodeJS.ProcessEnv; maxRawBytes?: number; limits?: Partial<FeedbackLimits> } = {}) {
    this.command = input.command;
    this.argsPrefix = [...(input.argsPrefix ?? [])];
    this.dedicatedOpenCommand = input.dedicatedOpenCommand ?? "lavish-open";
    this.dedicatedOpenArgsPrefix = [...(input.dedicatedOpenArgsPrefix ?? [])];
    this.cwd = input.cwd ?? process.cwd();
    this.env = { ...(input.env ?? {}) };
    this.maxRawBytes = input.maxRawBytes ?? 256_000;
    this.limits = { ...DEFAULT_FEEDBACK_LIMITS, ...(input.limits ?? {}) };
  }

  async open(file: string, input: { signal?: AbortSignal; reopen?: boolean; noOpen?: boolean } = {}) {
    if (input.noOpen) {
      const args = [file, ...(input.reopen ? ["--reopen"] : []), "--no-open"];
      const result = await this.run(args, input.signal);
      return { ...parseSession(result.stdout), raw: result.stdout };
    }
    const args = [...this.dedicatedOpenArgsPrefix, file, ...(input.reopen ? ["--reopen"] : [])];
    const result = await runProcess(this.dedicatedOpenCommand, args, {
      cwd: this.cwd,
      env: { ...process.env, ...this.env },
      signal: input.signal,
      maxBytes: this.maxRawBytes,
    });
    return { ...parseSession(result.stdout), raw: result.stdout };
  }

  async poll(file: string, input: { signal?: AbortSignal; agentReply?: string; timeoutMs?: number } = {}): Promise<LavishFeedback> {
    const args = ["poll", file, ...(input.agentReply ? ["--agent-reply", input.agentReply] : []), ...(input.timeoutMs !== undefined ? ["--timeout-ms", String(input.timeoutMs)] : [])];
    const result = await this.run(args, input.signal);
    return parsePollOutput(result.stdout, this.limits);
  }

  async end(file: string, input: { signal?: AbortSignal } = {}) {
    const result = await this.run(["end", file], input.signal);
    return { ...parseSession(result.stdout), raw: result.stdout };
  }

  async run(args: string[], signal?: AbortSignal): Promise<ProcessResult> {
    const executable = this.command ? { command: this.command, argsPrefix: this.argsPrefix } : resolvePinnedLavishCli();
    return runProcess(executable.command, [...executable.argsPrefix, ...args], {
      cwd: this.cwd,
      env: { ...process.env, ...this.env },
      signal,
      maxBytes: this.maxRawBytes,
    });
  }
}

export function resolvePinnedLavishCli(): { command: string; argsPrefix: string[] } {
  try {
    const require = createRequire(import.meta.url);
    const packageJson = require.resolve("lavish-axi/package.json");
    const manifest = JSON.parse(readFileSync(packageJson, "utf8")) as { bin?: string | Record<string, string> };
    const executable = typeof manifest.bin === "string" ? manifest.bin : manifest.bin?.["lavish-axi"];
    if (!executable) throw new Error("lavish-axi package does not declare its public CLI executable");
    return { command: process.execPath, argsPrefix: [resolve(dirname(packageJson), executable)] };
  } catch {
    throw new Error("Lavish presentation requires optional dependency lavish-axi@0.1.43; install package optional dependencies before using dag_model_present_review");
  }
}

export function parsePollOutput(raw: string, inputLimits: Partial<FeedbackLimits> = {}): LavishFeedback {
  const limits = { ...DEFAULT_FEEDBACK_LIMITS, ...inputLimits };
  const session = parseSession(raw);
  const parsedPrompts = parseRecordArray(raw, "prompts");
  const parsedWarnings = parseRecordArray(raw, "layout_warnings");
  const prompts: LavishPrompt[] = [];
  let originalBytes = 0;
  let returnedBytes = 0;
  let droppedPrompts = 0;
  let droppedBytes = 0;

  for (const source of parsedPrompts) {
    const normalized: LavishPrompt = {
      uid: string(source.uid),
      prompt: string(source.prompt),
      selector: string(source.selector),
      tag: string(source.tag),
      text: string(source.text),
      ...(isRecord(source.target) ? { target: source.target } : {}),
    };
    const originalRecordBytes = byteLength(JSON.stringify(normalized));
    originalBytes += originalRecordBytes;
    if (prompts.length >= limits.maxPrompts || returnedBytes >= limits.maxTotalBytes) { droppedPrompts += 1; droppedBytes += originalRecordBytes; continue; }
    const promptLimit = Math.max(0, Math.min(limits.maxPromptBytes, limits.maxTotalBytes - returnedBytes));
    const clipped = truncateUtf8(normalized.prompt, promptLimit);
    normalized.prompt = clipped.value;
    const normalizedBytes = byteLength(JSON.stringify(normalized));
    if (returnedBytes + normalizedBytes > limits.maxTotalBytes) { droppedPrompts += 1; droppedBytes += originalRecordBytes; continue; }
    if (clipped.truncated) droppedBytes += Math.max(0, originalRecordBytes - normalizedBytes);
    prompts.push(normalized);
    returnedBytes += normalizedBytes;
  }

  const layoutWarnings = parsedWarnings.slice(0, limits.maxWarnings).map((warning): LavishLayoutWarning => ({
    selector: string(warning.selector),
    kind: string(warning.kind),
    ...(warning.axis ? { axis: string(warning.axis) } : {}),
    overflowPx: number(warning.overflowPx),
    viewportWidth: number(warning.viewportWidth),
    severity: string(warning.severity),
    persistent: boolean(warning.persistent),
  })).filter(({ severity }) => severity === "error");

  return {
    session,
    prompts,
    layoutWarnings,
    truncation: {
      truncated: droppedPrompts > 0 || droppedBytes > 0 || parsedWarnings.length > limits.maxWarnings,
      originalPromptCount: parsedPrompts.length,
      returnedPromptCount: prompts.length,
      originalBytes,
      returnedBytes,
      droppedPrompts,
      droppedBytes,
      droppedWarnings: Math.max(0, parsedWarnings.length - limits.maxWarnings),
    },
  };
}

export function parseSession(raw: string): LavishSession {
  const record = parseIndentedRecord(sectionLines(raw, "session"), 2);
  return {
    file: string(record.file),
    status: string(record.status),
    ...(record.session_ended !== undefined ? { sessionEnded: boolean(record.session_ended) } : {}),
    ...(record.ended_by ? { endedBy: string(record.ended_by) } : {}),
  };
}

export function runProcess(command: string, args: string[], input: { cwd?: string; env?: NodeJS.ProcessEnv; signal?: AbortSignal; maxBytes?: number } = {}): Promise<ProcessResult> {
  return new Promise((resolvePromise, reject) => {
    let stdout: Buffer<ArrayBufferLike> = Buffer.alloc(0);
    let stderr: Buffer<ArrayBufferLike> = Buffer.alloc(0);
    let overflow = false;
    const maxBytes = input.maxBytes ?? 256_000;
    const child = spawn(command, args, { cwd: input.cwd, env: input.env, signal: input.signal, stdio: ["ignore", "pipe", "pipe"] });
    const collect = (target: Buffer<ArrayBufferLike>, chunk: Buffer<ArrayBufferLike>) => {
      if (stdout.length + stderr.length + chunk.length > maxBytes) {
        overflow = true;
        child.kill("SIGTERM");
        return target;
      }
      return Buffer.concat([target, chunk]);
    };
    child.stdout.on("data", (chunk: Buffer<ArrayBufferLike>) => { stdout = collect(stdout, chunk); });
    child.stderr.on("data", (chunk: Buffer<ArrayBufferLike>) => { stderr = collect(stderr, chunk); });
    child.on("error", reject);
    child.on("close", (code, closeSignal) => {
      if (overflow) { reject(new Error(`Lavish CLI output exceeded ${maxBytes} bytes`)); return; }
      const result: ProcessResult = { code: code ?? -1, signal: closeSignal, stdout: stdout.toString("utf8"), stderr: stderr.toString("utf8") };
      if (code !== 0) {
        const error = new Error(`Lavish CLI failed (${code ?? closeSignal}): ${result.stderr || result.stdout}`.trim()) as Error & { result?: ProcessResult };
        error.result = result;
        reject(error);
        return;
      }
      resolvePromise(result);
    });
  });
}

function parseRecordArray(raw: string, name: string): Array<Record<string, unknown>> {
  const lines = raw.split(/\r?\n/);
  const headerPattern = new RegExp(`^${escapeRegex(name)}\\[(\\d+)\\](?:\\{([^}]+)\\})?:\\s*$`);
  const start = lines.findIndex((line) => headerPattern.test(line));
  if (start < 0) return [];
  const columns = lines[start].match(headerPattern)?.[2]?.split(",").map((column) => column.trim()).filter(Boolean) ?? [];
  if (columns.length) {
    const rows: Array<Record<string, unknown>> = [];
    for (let index = start + 1; index < lines.length; index++) {
      const line = lines[index];
      if (/^[A-Za-z_][A-Za-z0-9_]*(?:\[\d+\])?(?:\{[^}]+\})?:/.test(line)) break;
      if (!line.startsWith("  ") || !line.trim()) continue;
      const values = splitToonRow(line.slice(2));
      rows.push(Object.fromEntries(columns.map((column, columnIndex) => [column, scalar(values[columnIndex] ?? "")])));
    }
    return rows;
  }
  const output: Array<Record<string, unknown>> = [];
  let current: Record<string, unknown> | undefined;
  let nestedKey: string | undefined;
  for (let index = start + 1; index < lines.length; index++) {
    const line = lines[index];
    if (/^[A-Za-z_][A-Za-z0-9_]*(?:\[\d+\])?(?:\{[^}]+\})?:/.test(line)) break;
    const first = line.match(/^  - ([A-Za-z_][A-Za-z0-9_]*):\s*(.*)$/);
    if (first) { current = { [first[1]]: scalar(first[2]) }; output.push(current); nestedKey = undefined; continue; }
    if (!current) continue;
    const field = line.match(/^    ([A-Za-z_][A-Za-z0-9_]*):\s*(.*)$/);
    if (field) {
      if (field[2] === "") { nestedKey = field[1]; current[nestedKey] = {}; }
      else { current[field[1]] = scalar(field[2]); nestedKey = undefined; }
      continue;
    }
    const nested = line.match(/^      ([A-Za-z_][A-Za-z0-9_]*):\s*(.*)$/);
    const nestedTarget = nestedKey ? current[nestedKey] : undefined;
    if (nested && isRecord(nestedTarget)) nestedTarget[nested[1]] = scalar(nested[2]);
  }
  return output;
}

function sectionLines(raw: string, name: string): string[] {
  const lines = raw.split(/\r?\n/);
  const start = lines.findIndex((line) => line === `${name}:`);
  if (start < 0) return [];
  const output: string[] = [];
  for (let index = start + 1; index < lines.length; index++) {
    if (/^[A-Za-z_][A-Za-z0-9_]*(?:\[\d+\])?(?:\{[^}]+\})?:/.test(lines[index])) break;
    output.push(lines[index]);
  }
  return output;
}
function parseIndentedRecord(lines: string[], indent: number): Record<string, unknown> {
  const record: Record<string, unknown> = {};
  const prefix = " ".repeat(indent);
  for (const line of lines) {
    const match = line.match(new RegExp(`^${prefix}([A-Za-z_][A-Za-z0-9_]*):\\s*(.*)$`));
    if (match) record[match[1]] = scalar(match[2]);
  }
  return record;
}
function scalar(value: string): unknown {
  const trimmed = value.trim();
  if (trimmed === "true") return true;
  if (trimmed === "false") return false;
  if (trimmed === "null") return null;
  if (/^-?\d+(?:\.\d+)?$/.test(trimmed)) return Number(trimmed);
  if (trimmed.startsWith('"')) { try { return JSON.parse(trimmed); } catch {} }
  return trimmed;
}
function splitToonRow(value: string): string[] {
  const fields: string[] = [];
  let current = "";
  let quoted = false;
  let escaped = false;
  for (const char of value) {
    if (escaped) { current += char; escaped = false; continue; }
    if (char === "\\" && quoted) { current += char; escaped = true; continue; }
    if (char === '"') { current += char; quoted = !quoted; continue; }
    if (char === "," && !quoted) { fields.push(current.trim()); current = ""; continue; }
    current += char;
  }
  fields.push(current.trim());
  return fields;
}
function truncateUtf8(value: string, maxBytes: number) {
  const input = Buffer.from(value);
  if (input.length <= maxBytes) return { value, truncated: false };
  if (maxBytes <= 3) return { value: "".padEnd(Math.max(0, maxBytes), "."), truncated: true };
  let end = maxBytes - 3;
  while (end > 0 && (input[end] & 0b1100_0000) === 0b1000_0000) end -= 1;
  return { value: `${input.subarray(0, end).toString("utf8")}...`, truncated: true };
}
function isRecord(value: unknown): value is Record<string, unknown> { return Boolean(value) && typeof value === "object" && !Array.isArray(value); }
function byteLength(value: string) { return Buffer.byteLength(value); }
function string(value: unknown) { return value === undefined || value === null ? "" : String(value); }
function number(value: unknown) { const parsed = Number(value); return Number.isFinite(parsed) ? parsed : 0; }
function boolean(value: unknown) { return value === true || value === "true"; }
function escapeRegex(value: string) { return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"); }
