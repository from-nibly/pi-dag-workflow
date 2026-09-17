#!/usr/bin/env node
// Deterministic generic RPC worker: executes a real tracked implementation script,
// then reports the observed exit. The production supervisor owns result ingestion.
import { createInterface } from "node:readline";
import { spawnSync } from "node:child_process";
const emit = value => process.stdout.write(`${JSON.stringify(value)}\n`);
const env = { ...Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith("GIT_"))), GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_SYSTEM: "/dev/null", GIT_CONFIG_GLOBAL: "/dev/null", GIT_TERMINAL_PROMPT: "0", GIT_NO_LAZY_FETCH: "1" };
let handled = false;
createInterface({ input: process.stdin }).on("line", line => {
  const command = JSON.parse(line);
  if (command.type === "abort") { emit({ type: "response", id: command.id, command: "abort", success: true }); process.exit(0); }
  if (command.type !== "prompt" || handled) return;
  handled = true;
  emit({ type: "response", id: command.id, command: "prompt", success: true });
  const item = JSON.parse(command.message.match(/^Item: (.+)$/m)?.[1] ?? "null");
  if (item?.context.includes("wait-for-cancel")) return;
  const result = spawnSync(process.execPath, ["implement.mjs", item?.id ?? "missing", command.message.includes("\nRepair observations: ") ? "repair" : "initial"], { cwd: process.cwd(), env, encoding: "utf8", timeout: 10000 });
  const report = { outcome: result.status === 0 ? "completed" : "needs_attention", summary: `Implementation command exit ${result.status}`, details: `${result.stdout ?? ""}\n${result.stderr ?? ""}`.slice(-8192) };
  emit({ type: "tool_execution_end", toolCallId: "report", toolName: "subagent_report", isError: false, result: { content: [{ type: "text", text: report.summary }], details: { schemaVersion: 1, report }, terminate: true } });
  emit({ type: "message_end", message: { role: "assistant", content: [{ type: "toolCall", name: "subagent_report" }], model: "deterministic", provider: "local", stopReason: "toolUse", usage: { input: 0, output: 0, totalTokens: 0, cost: { total: 0 } } } });
  emit({ type: "agent_settled" });
});
process.stdin.on("end", () => process.exit(0));
