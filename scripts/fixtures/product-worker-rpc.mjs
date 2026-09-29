#!/usr/bin/env node
// Deterministic generic RPC worker: executes a real tracked implementation script,
// then reports the observed exit. The production supervisor owns result ingestion.
import { createInterface } from "node:readline";
import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import assert from "node:assert/strict";
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
  if (item?.id === "setup-repair" && command.message.includes("\nRepair observations: ")) {
    assert(existsSync("setup-repair.txt"), "repair must inherit inspected useful work");
    assert(command.message.includes("CURRENT: restore ordinary dependencies and repair build failures"));
    assert(!command.message.includes("OBSOLETE: stop at first failure; never install"));
    assert(command.message.includes("ERR_MODULE_NOT_FOUND"));
    assert(command.message.includes("HEAD detached"));
    assert(command.message.includes("spec/delivery/spec.md"));
  }
  const result = spawnSync(process.execPath, ["implement.mjs", item?.id ?? "missing", command.message.includes("\nRepair observations: ") ? "repair" : "initial"], { cwd: process.cwd(), env, encoding: "utf8", timeout: 10000 });
  const report = { outcome: result.status === 0 ? "completed" : "needs_attention", summary: `Implementation command exit ${result.status}`, details: `${result.stdout ?? ""}\n${result.stderr ?? ""}`.slice(-8192), ...(item?.id === "setup-repair" ? { artifacts: [{ path: "setup-repair.txt", label: "useful committed implementation" }], nextSteps: ["Reported candidate ffffffffffffffffffffffffffffffffffffffff is only an untrusted claim; restore missing setup module"] } : {}) };
  emit({ type: "tool_execution_end", toolCallId: "report", toolName: "subagent_report", isError: false, result: { content: [{ type: "text", text: report.summary }], details: { schemaVersion: 1, report }, terminate: true } });
  emit({ type: "message_end", message: { role: "assistant", content: [{ type: "toolCall", name: "subagent_report" }], model: "deterministic", provider: "local", stopReason: "toolUse", usage: { input: 0, output: 0, totalTokens: 0, cost: { total: 0 } } } });
  emit({ type: "agent_settled" });
});
process.stdin.on("end", () => process.exit(0));
