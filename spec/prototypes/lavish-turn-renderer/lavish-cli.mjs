import { spawn } from "node:child_process";

export const DEFAULT_FEEDBACK_LIMITS = Object.freeze({ maxPrompts: 20, maxPromptBytes: 4_000, maxTotalBytes: 16_000, maxWarnings: 20 });

export class LavishCliAdapter {
  constructor({ command = process.execPath, argsPrefix = [], cwd = process.cwd(), env = {}, maxRawBytes = 256_000, limits = {} } = {}) {
    this.command = command;
    this.argsPrefix = [...argsPrefix];
    this.cwd = cwd;
    this.env = { ...env };
    this.maxRawBytes = maxRawBytes;
    this.limits = { ...DEFAULT_FEEDBACK_LIMITS, ...limits };
  }

  async open(file, { signal, reopen = false, noOpen = false } = {}) {
    const args = [file, ...(reopen ? ["--reopen"] : []), ...(noOpen ? ["--no-open"] : [])];
    const result = await this.run(args, { signal });
    return { ...parseSession(result.stdout), raw: result.stdout };
  }

  async poll(file, { signal, agentReply, timeoutMs } = {}) {
    const args = ["poll", file, ...(agentReply ? ["--agent-reply", agentReply] : []), ...(timeoutMs !== undefined ? ["--timeout-ms", String(timeoutMs)] : [])];
    const result = await this.run(args, { signal });
    return parsePollOutput(result.stdout, this.limits);
  }

  async end(file, { signal } = {}) {
    const result = await this.run(["end", file], { signal });
    return { ...parseSession(result.stdout), raw: result.stdout };
  }

  async run(args, { signal } = {}) {
    return runProcess(this.command, [...this.argsPrefix, ...args], {
      cwd: this.cwd,
      env: { ...process.env, ...this.env },
      signal,
      maxBytes: this.maxRawBytes,
    });
  }
}

export function parsePollOutput(raw, inputLimits = {}) {
  const limits = { ...DEFAULT_FEEDBACK_LIMITS, ...inputLimits };
  const session = parseSession(raw);
  const parsedPrompts = parseRecordArray(raw, "prompts");
  const parsedWarnings = parseRecordArray(raw, "layout_warnings");
  const prompts = [];
  let originalBytes = 0;
  let returnedBytes = 0;
  let truncatedPromptCount = 0;
  let truncatedBytes = 0;

  for (const source of parsedPrompts) {
    const normalized = {
      uid: string(source.uid),
      prompt: string(source.prompt),
      selector: string(source.selector),
      tag: string(source.tag),
      text: string(source.text),
      ...(source.target && typeof source.target === "object" ? { target: source.target } : {}),
    };
    const bytes = byteLength(JSON.stringify(normalized));
    originalBytes += bytes;
    if (prompts.length >= limits.maxPrompts || returnedBytes >= limits.maxTotalBytes) { truncatedPromptCount += 1; truncatedBytes += bytes; continue; }
    const promptLimit = Math.max(0, Math.min(limits.maxPromptBytes, limits.maxTotalBytes - returnedBytes));
    const clipped = truncateUtf8(normalized.prompt, promptLimit);
    if (clipped.truncated) truncatedBytes += byteLength(normalized.prompt) - byteLength(clipped.value);
    normalized.prompt = clipped.value;
    const normalizedBytes = byteLength(JSON.stringify(normalized));
    if (returnedBytes + normalizedBytes > limits.maxTotalBytes) { truncatedPromptCount += 1; truncatedBytes += bytes; continue; }
    prompts.push(normalized);
    returnedBytes += normalizedBytes;
  }

  const layoutWarnings = parsedWarnings.slice(0, limits.maxWarnings).map((warning) => ({
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
      truncated: truncatedPromptCount > 0 || truncatedBytes > 0 || parsedWarnings.length > limits.maxWarnings,
      originalPromptCount: parsedPrompts.length,
      returnedPromptCount: prompts.length,
      originalBytes,
      returnedBytes,
      droppedPrompts: truncatedPromptCount,
      droppedBytes: truncatedBytes,
      droppedWarnings: Math.max(0, parsedWarnings.length - limits.maxWarnings),
    },
  };
}

export function parseSession(raw) {
  const sessionBlock = sectionLines(raw, "session");
  const record = parseIndentedRecord(sessionBlock, 2);
  return {
    file: string(record.file),
    status: string(record.status),
    ...(record.session_ended !== undefined ? { sessionEnded: boolean(record.session_ended) } : {}),
    ...(record.ended_by ? { endedBy: string(record.ended_by) } : {}),
  };
}

export function runProcess(command, args, { cwd, env, signal, maxBytes = 256_000 } = {}) {
  return new Promise((resolve, reject) => {
    let stdout = Buffer.alloc(0);
    let stderr = Buffer.alloc(0);
    let overflow = false;
    const child = spawn(command, args, { cwd, env, signal, stdio: ["ignore", "pipe", "pipe"] });
    const collect = (target, chunk) => {
      const next = Buffer.concat([target, chunk]);
      if (stdout.length + stderr.length + chunk.length > maxBytes) {
        overflow = true;
        child.kill("SIGTERM");
        return target;
      }
      return next;
    };
    child.stdout.on("data", (chunk) => { stdout = collect(stdout, chunk); });
    child.stderr.on("data", (chunk) => { stderr = collect(stderr, chunk); });
    child.on("error", (error) => reject(error));
    child.on("close", (code, closeSignal) => {
      if (overflow) { reject(new Error(`Lavish CLI output exceeded ${maxBytes} bytes`)); return; }
      const result = { code: code ?? -1, signal: closeSignal, stdout: stdout.toString("utf8"), stderr: stderr.toString("utf8") };
      if (code !== 0) {
        const error = new Error(`Lavish CLI failed (${code ?? closeSignal}): ${result.stderr || result.stdout}`.trim());
        error.result = result;
        reject(error);
        return;
      }
      resolve(result);
    });
  });
}

function parseRecordArray(raw, name) {
  const lines = String(raw).split(/\r?\n/);
  const headerPattern = new RegExp(`^${escapeRegex(name)}\\[(\\d+)\\](?:\\{([^}]+)\\})?:\\s*$`);
  const start = lines.findIndex((line) => headerPattern.test(line));
  if (start < 0) return [];
  const header = lines[start].match(headerPattern);
  const columns = header?.[2]?.split(",").map((column) => column.trim()).filter(Boolean) ?? [];
  if (columns.length) {
    const rows = [];
    for (let index = start + 1; index < lines.length; index++) {
      const line = lines[index];
      if (/^[A-Za-z_][A-Za-z0-9_]*(?:\[\d+\])?(?:\{[^}]+\})?:/.test(line)) break;
      if (!line.startsWith("  ") || !line.trim()) continue;
      const values = splitToonRow(line.slice(2));
      rows.push(Object.fromEntries(columns.map((column, columnIndex) => [column, scalar(values[columnIndex] ?? "")])));
    }
    return rows;
  }
  const output = [];
  let current;
  let nestedKey;
  for (let index = start + 1; index < lines.length; index++) {
    const line = lines[index];
    if (/^[A-Za-z_][A-Za-z0-9_]*(?:\[\d+\])?(?:\{[^}]+\})?:/.test(line)) break;
    const first = line.match(/^  - ([A-Za-z_][A-Za-z0-9_]*):\s*(.*)$/);
    if (first) {
      current = { [first[1]]: scalar(first[2]) };
      output.push(current);
      nestedKey = undefined;
      continue;
    }
    if (!current) continue;
    const field = line.match(/^    ([A-Za-z_][A-Za-z0-9_]*):\s*(.*)$/);
    if (field) {
      if (field[2] === "") { nestedKey = field[1]; current[nestedKey] = {}; }
      else { current[field[1]] = scalar(field[2]); nestedKey = undefined; }
      continue;
    }
    const nested = line.match(/^      ([A-Za-z_][A-Za-z0-9_]*):\s*(.*)$/);
    if (nested && nestedKey) current[nestedKey][nested[1]] = scalar(nested[2]);
  }
  return output;
}

function sectionLines(raw, name) {
  const lines = String(raw).split(/\r?\n/);
  const start = lines.findIndex((line) => line === `${name}:`);
  if (start < 0) return [];
  const output = [];
  for (let index = start + 1; index < lines.length; index++) {
    if (/^[A-Za-z_][A-Za-z0-9_]*(?:\[\d+\])?:/.test(lines[index])) break;
    output.push(lines[index]);
  }
  return output;
}
function parseIndentedRecord(lines, indent) {
  const record = {};
  const prefix = " ".repeat(indent);
  for (const line of lines) {
    const match = line.match(new RegExp(`^${prefix}([A-Za-z_][A-Za-z0-9_]*):\\s*(.*)$`));
    if (match) record[match[1]] = scalar(match[2]);
  }
  return record;
}
function scalar(value) {
  const trimmed = String(value).trim();
  if (trimmed === "true") return true;
  if (trimmed === "false") return false;
  if (trimmed === "null") return null;
  if (/^-?\d+(?:\.\d+)?$/.test(trimmed)) return Number(trimmed);
  if (trimmed.startsWith('"')) { try { return JSON.parse(trimmed); } catch {} }
  return trimmed;
}
function splitToonRow(value) {
  const fields = [];
  let current = "";
  let quoted = false;
  let escaped = false;
  for (const char of String(value)) {
    if (escaped) { current += char; escaped = false; continue; }
    if (char === "\\" && quoted) { current += char; escaped = true; continue; }
    if (char === '"') { current += char; quoted = !quoted; continue; }
    if (char === "," && !quoted) { fields.push(current.trim()); current = ""; continue; }
    current += char;
  }
  fields.push(current.trim());
  return fields;
}
function truncateUtf8(value, maxBytes) {
  const input = Buffer.from(String(value));
  if (input.length <= maxBytes) return { value: String(value), truncated: false };
  if (maxBytes <= 3) return { value: "".padEnd(maxBytes, "."), truncated: true };
  let end = maxBytes - 3;
  while (end > 0 && (input[end] & 0b1100_0000) === 0b1000_0000) end -= 1;
  return { value: `${input.subarray(0, end).toString("utf8")}...`, truncated: true };
}
function byteLength(value) { return Buffer.byteLength(String(value)); }
function string(value) { return value === undefined || value === null ? "" : String(value); }
function number(value) { const parsed = Number(value); return Number.isFinite(parsed) ? parsed : 0; }
function boolean(value) { return value === true || value === "true"; }
function escapeRegex(value) { return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&"); }
