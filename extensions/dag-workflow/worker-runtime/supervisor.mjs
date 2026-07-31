#!/usr/bin/env node
import { spawn } from "node:child_process";
import { readFile } from "node:fs/promises";
import { relative } from "node:path";
import {
  BoundedDiagnosticLog,
  MAX_FALLBACK_TEXT_BYTES,
  MAX_MAILBOX_BYTES,
  MAX_REPORT_BYTES,
  StrictJsonlParser,
  WORKER_MAILBOX_SCHEMA_VERSION,
  WORKER_RESULT_SCHEMA_VERSION,
  assertAttemptConfig,
  atomicWriteJson,
  attemptPaths,
  newNonce,
  nowIso,
  pathExists,
  processStartIdentity,
  sha256,
  withResultHash,
  writeImmutableJson,
} from "./core.mjs";

const configPath = process.argv[2];
if (!configPath) throw new Error("Usage: supervisor.mjs <attempt-config.json>");
const config = JSON.parse(await readFile(configPath, "utf8"));
assertAttemptConfig(config);
const paths = attemptPaths(config.repositoryRoot, config.storageId, config.workerId, config.attemptNumber);
const startedAt = nowIso();
const supervisorIdentity = await processStartIdentity();
const diagnostics = new BoundedDiagnosticLog(paths.diagnostics);
let child;
let childIdentity = null;
let lastAssistantText = "";
let lastModel = null;
let lastProvider = null;
let lastStopReason = null;
let report = null;
let repairsUsed = 0;
let modelError = null;
let protocolError = null;
let cancelRequested = false;
let terminalIntent = null;
let teardownForced = false;
let finalized = false;
let recordQueue = Promise.resolve();
let mailboxQueue = Promise.resolve();
let heartbeatTimer;
let cancelTimer;
let forceTimer;
const usage = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: 0 };
const eventCounts = {};

await writeMailbox("starting");
child = spawn(process.execPath, buildPiArgs(config), {
  cwd: config.cwd,
  env: {
    ...process.env,
    PI_DAG_WORKER_ROLE: "child",
    PI_DAG_WORKER_CONFIG: configPath,
    PI_DAG_WORKER_ATTEMPT_NONCE: config.attemptNonce,
  },
  stdio: ["pipe", "pipe", "pipe"],
});
childIdentity = await waitForProcessIdentity(child.pid);
await writeMailbox("running");
heartbeatTimer = setInterval(() => { void writeMailbox(terminalIntent ? "settling" : "running"); }, 2000);
cancelTimer = setInterval(() => { void pollCancellation(); }, 400);

const parser = new StrictJsonlParser(
  (record) => {
    recordQueue = recordQueue.then(() => handleRecord(record)).catch((error) => { protocolError = `RPC event handling failed: ${error.message}`; });
  },
  (error, line) => {
    recordQueue = recordQueue.then(async () => {
      protocolError = `Invalid RPC JSON: ${error.message}`;
      await diagnostics.append({ type: "rpc_parse_error", timestamp: nowIso(), line: truncateUtf8(line, 4096), error: error.message });
    }).catch((writeError) => { protocolError = `RPC parse diagnostics failed: ${writeError.message}`; });
  },
);
child.stdout.on("data", (chunk) => parser.push(chunk));
child.stdout.on("end", () => parser.end());
child.stderr.on("data", (chunk) => { void diagnostics.append({ type: "rpc_stderr", timestamp: nowIso(), text: truncateUtf8(String(chunk), 8192) }); });
child.on("error", (error) => {
  protocolError = `Pi RPC spawn error: ${error.message}`;
  void diagnostics.append({ type: "rpc_process_error", timestamp: nowIso(), error: error.message });
});
child.on("exit", (code, signal) => { void finalize(code, signal); });

process.on("SIGTERM", () => { void requestCancellation("supervisor_sigterm"); });
process.on("SIGINT", () => { void requestCancellation("supervisor_sigint"); });

send({ id: "initial-prompt", type: "prompt", message: buildInitialPrompt(config) });

async function handleRecord(record) {
  if (!record || typeof record !== "object" || typeof record.type !== "string") return;
  eventCounts[record.type] = (eventCounts[record.type] ?? 0) + 1;
  if (record.type !== "message_update" && record.type !== "tool_execution_update") await diagnostics.append(projectDiagnostic(record));

  if (record.type === "extension_ui_request") {
    if (["select", "confirm", "input", "editor"].includes(record.method)) send({ type: "extension_ui_response", id: record.id, cancelled: true });
    return;
  }
  if (record.type === "response" && record.success === false) {
    protocolError = `RPC ${record.command ?? "command"} failed: ${record.error ?? "unknown error"}`;
    return;
  }
  if (record.type === "message_end" && record.message?.role === "assistant") captureAssistant(record.message);
  if (record.type === "tool_execution_end" && record.toolName === "subagent_report" && !record.isError) {
    const candidate = record.result?.details?.report;
    if (validReport(candidate)) report = structuredClone(candidate);
  }
  if (record.type === "agent_settled") await handleSettled();
}

async function handleSettled() {
  if (cancelRequested) {
    terminalIntent = "cancelled";
    return closeChildAfterSettle();
  }
  if (report) {
    terminalIntent = report.outcome === "completed" ? "succeeded" : "needs_attention";
    return closeChildAfterSettle();
  }
  if (repairsUsed < config.reportRepairAttempts) {
    repairsUsed += 1;
    send({ id: `report-repair-${repairsUsed}`, type: "prompt", message: buildRepairPrompt(repairsUsed) });
    return;
  }
  terminalIntent = "needs_attention";
  closeChildAfterSettle();
}

function closeChildAfterSettle() {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  child.stdin.end();
  forceTimer = setTimeout(() => {
    if (child.exitCode === null && child.signalCode === null) {
      teardownForced = true;
      child.kill("SIGTERM");
    }
  }, 2000);
}

async function pollCancellation() {
  if (cancelRequested || !(await pathExists(paths.cancel))) return;
  try {
    const cancel = JSON.parse(await readFile(paths.cancel, "utf8"));
    if (cancel.attemptNonce !== config.attemptNonce || cancel.configHash !== config.configHash) {
      protocolError = "Cancellation mailbox identity mismatch";
      await diagnostics.append({ type: "cancel_identity_mismatch", timestamp: nowIso() });
      return;
    }
    await requestCancellation(cancel.reason ?? "requested");
  } catch (error) {
    protocolError = `Invalid cancellation mailbox: ${error.message}`;
  }
}

async function requestCancellation(reason) {
  if (cancelRequested) return;
  cancelRequested = true;
  await diagnostics.append({ type: "cancellation_requested", timestamp: nowIso(), reason });
  send({ id: `abort-${Date.now()}`, type: "abort" });
  forceTimer = setTimeout(() => {
    if (child?.exitCode === null && child?.signalCode === null) {
      teardownForced = true;
      child.kill("SIGTERM");
    }
  }, 5000);
}

function send(command) {
  if (!child?.stdin || child.stdin.destroyed) return;
  try { child.stdin.write(`${JSON.stringify(command)}\n`); }
  catch (error) { protocolError = `RPC write failed: ${error.message}`; }
}

function captureAssistant(message) {
  const text = (Array.isArray(message.content) ? message.content : [])
    .filter((part) => part?.type === "text" && typeof part.text === "string")
    .map((part) => part.text)
    .join("\n")
    .trim();
  if (text) lastAssistantText = truncateUtf8(text, MAX_FALLBACK_TEXT_BYTES);
  lastModel = typeof message.model === "string" ? message.model : lastModel;
  lastProvider = typeof message.provider === "string" ? message.provider : lastProvider;
  lastStopReason = typeof message.stopReason === "string" ? message.stopReason : lastStopReason;
  if (message.stopReason === "error" || message.errorMessage) modelError = String(message.errorMessage ?? "model stop reason error");
  const item = message.usage;
  if (item && typeof item === "object") {
    for (const key of ["input", "output", "cacheRead", "cacheWrite", "totalTokens"]) usage[key] += Number(item[key] ?? 0) || 0;
    usage.cost += Number(item.cost?.total ?? item.cost ?? 0) || 0;
  }
}

async function finalize(exitCode, signal) {
  if (finalized) return;
  finalized = true;
  await recordQueue;
  clearInterval(heartbeatTimer);
  clearInterval(cancelTimer);
  clearTimeout(forceTimer);
  await diagnostics.flush();

  let terminalStatus = terminalIntent;
  if (cancelRequested) terminalStatus = "cancelled";
  else if (modelError || protocolError || (!terminalIntent && (exitCode !== 0 || signal)) || (!terminalIntent && !report)) terminalStatus = "failed";
  else if (exitCode !== 0 && !teardownForced) terminalStatus = "failed";
  if (!terminalStatus) terminalStatus = "failed";
  const reportStatus = report ? (repairsUsed ? "repaired" : "valid") : "missing";
  if (reportStatus === "missing" && terminalStatus === "succeeded") terminalStatus = "needs_attention";

  const completionSeed = sha256({ storageId: config.storageId, workerId: config.workerId, attemptNumber: config.attemptNumber, attemptNonce: config.attemptNonce, configHash: config.configHash });
  const result = withResultHash({
    schemaVersion: WORKER_RESULT_SCHEMA_VERSION,
    completionId: `completion-${config.workerId}-${config.attemptNumber}-${completionSeed.slice(7, 19)}`,
    storageId: config.storageId,
    ownerSessionId: config.ownerSessionId,
    workerId: config.workerId,
    attemptNumber: config.attemptNumber,
    attemptNonce: config.attemptNonce,
    configHash: config.configHash,
    terminalStatus,
    reportStatus,
    ...(report ? { report } : {}),
    startedAt,
    endedAt: nowIso(),
    process: { supervisorPid: process.pid, supervisorStartIdentity: supervisorIdentity, childPid: child?.pid ?? null, childStartIdentity: childIdentity, exitCode, signal, teardownForced },
    runtime: { model: lastModel, provider: lastProvider, stopReason: lastStopReason, usage, repairsUsed, modelError, protocolError },
    ...(lastAssistantText ? { fallbackFinalText: lastAssistantText } : {}),
    diagnostics: { path: relative(config.repositoryRoot, paths.diagnostics), cappedAtBytes: diagnostics.maxBytes, truncated: diagnostics.truncated, eventCounts },
    artifacts: report?.artifacts ?? [],
  });
  try {
    await writeImmutableJson(paths.result, result);
    await writeMailbox("result_written", { completionId: result.completionId, terminalStatus, resultHash: result.resultHash });
  } catch (error) {
    await writeMailbox("result_write_failed", { error: truncateUtf8(error.message, 2048) }).catch(() => {});
    process.exitCode = 1;
    return;
  }
  process.exitCode = 0;
}

function writeMailbox(status, extra = {}) {
  mailboxQueue = mailboxQueue.then(async () => {
    await atomicWriteJson(paths.mailbox, {
      schemaVersion: WORKER_MAILBOX_SCHEMA_VERSION,
      storageId: config.storageId,
      ownerSessionId: config.ownerSessionId,
      workerId: config.workerId,
      attemptNumber: config.attemptNumber,
      attemptNonce: config.attemptNonce,
      configHash: config.configHash,
      supervisorPid: process.pid,
      supervisorStartIdentity: supervisorIdentity,
      childPid: child?.pid ?? null,
      childStartIdentity: childIdentity,
      status,
      heartbeatAt: nowIso(),
      eventCounts,
      ...extra,
    }, { maxBytes: MAX_MAILBOX_BYTES });
  });
  return mailboxQueue;
}

function buildPiArgs(input) {
  const args = [input.piCliPath, "--mode", "rpc", "--no-session", "--name", `worker:${input.workerId}:${input.attemptNumber}`, "--approve"];
  if (input.extensionPath) args.push("--extension", input.extensionPath);
  if (input.provider) args.push("--provider", input.provider);
  if (input.model) args.push("--model", input.model);
  if (input.thinking) args.push("--thinking", input.thinking);
  const tools = [...new Set([...input.activeTools.filter((name) => !isOmittedTool(name)), "subagent_report"])];
  if (tools.length) args.push("--tools", tools.join(","));
  return args;
}

function isOmittedTool(name) {
  return name === "subagent" || name.startsWith("subagent_") || name.startsWith("dag_") || name.startsWith("dag_model_");
}

function buildInitialPrompt(input) {
  return `You are worker ${input.workerId}, attempt ${input.attemptNumber}.\n\nTask:\n${input.task}\n\nWork autonomously in ${input.cwd}. Do not delegate to other workers or use DAG/project-model orchestration. When complete or blocked, call subagent_report as your final action with a concise summary. Do not finish with ordinary assistant text alone.`;
}

function buildRepairPrompt(attempt) {
  return `REPORT REPAIR ${attempt}/${config.reportRepairAttempts}: Do not perform additional task work. Your previous run settled without a valid terminal report. Call subagent_report now as your only action. Use outcome=completed only if the assigned task is complete; otherwise use needs_attention. Include a concise summary and optional bounded details, artifact references, and next steps.`;
}

function validReport(value) {
  if (!value || typeof value !== "object") return false;
  if (!new Set(["completed", "needs_attention"]).has(value.outcome)) return false;
  if (typeof value.summary !== "string" || !value.summary.trim() || value.summary.length > 8192) return false;
  if (value.details !== undefined && (typeof value.details !== "string" || value.details.length > 32768)) return false;
  if (value.artifacts !== undefined && (!Array.isArray(value.artifacts) || value.artifacts.length > 32 || value.artifacts.some((artifact) => !artifact || typeof artifact.path !== "string" || !artifact.path || artifact.path.length > 4096 || (artifact.label !== undefined && (typeof artifact.label !== "string" || artifact.label.length > 256))))) return false;
  if (value.nextSteps !== undefined && (!Array.isArray(value.nextSteps) || value.nextSteps.length > 16 || value.nextSteps.some((step) => typeof step !== "string" || !step || step.length > 1024))) return false;
  return Buffer.byteLength(JSON.stringify(value)) <= MAX_REPORT_BYTES;
}

function projectDiagnostic(record) {
  const base = { type: record.type, timestamp: nowIso() };
  if (record.type === "message_end") {
    const message = record.message ?? {};
    return { ...base, role: message.role, model: message.model, provider: message.provider, stopReason: message.stopReason, text: message.role === "assistant" ? assistantText(message) : undefined, errorMessage: message.errorMessage };
  }
  if (record.type === "tool_execution_start") return { ...base, toolCallId: record.toolCallId, toolName: record.toolName, args: truncateValue(record.args, 8192) };
  if (record.type === "tool_execution_end") return { ...base, toolCallId: record.toolCallId, toolName: record.toolName, isError: record.isError, result: truncateValue(record.result, record.toolName === "subagent_report" ? MAX_REPORT_BYTES + 4096 : 8192) };
  if (record.type === "response") return { ...base, id: record.id, command: record.command, success: record.success, error: truncateUtf8(String(record.error ?? ""), 4096) };
  if (record.type === "extension_error") return { ...base, extensionPath: record.extensionPath, event: record.event, error: truncateUtf8(String(record.error ?? ""), 4096) };
  if (record.type === "extension_ui_request") return { ...base, id: record.id, method: record.method, title: truncateUtf8(String(record.title ?? ""), 1024) };
  return base;
}

function assistantText(message) {
  return truncateUtf8((Array.isArray(message.content) ? message.content : []).filter((part) => part?.type === "text").map((part) => part.text).join("\n"), 8192);
}

function truncateValue(value, maxBytes) {
  const text = JSON.stringify(value);
  if (Buffer.byteLength(text) <= maxBytes) return value;
  return { truncated: true, preview: truncateUtf8(text, maxBytes) };
}

function truncateUtf8(value, maxBytes) {
  const buffer = Buffer.from(String(value ?? ""));
  if (buffer.length <= maxBytes) return buffer.toString();
  return `${buffer.subarray(0, Math.max(0, maxBytes - 32)).toString()}\n…[truncated]`;
}

async function waitForProcessIdentity(pid) {
  for (let attempt = 0; attempt < 20; attempt++) {
    const identity = await processStartIdentity(pid);
    if (identity) return identity;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  return null;
}
