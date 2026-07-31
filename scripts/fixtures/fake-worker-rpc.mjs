#!/usr/bin/env node
import { StringDecoder } from "node:string_decoder";

const mode = process.env.FAKE_WORKER_RPC_MODE ?? "valid";
const decoder = new StringDecoder("utf8");
let buffer = "";
let prompts = 0;
process.stdin.on("data", (chunk) => {
  buffer += decoder.write(chunk);
  while (true) {
    const newline = buffer.indexOf("\n");
    if (newline < 0) break;
    const line = buffer.slice(0, newline).replace(/\r$/, "");
    buffer = buffer.slice(newline + 1);
    if (line.trim()) handle(JSON.parse(line));
  }
});
process.stdin.on("end", () => process.exit(0));

function emit(value) { process.stdout.write(`${JSON.stringify(value)}\n`); }
function handle(command) {
  if (command.type === "extension_ui_response") {
    respondWithReport();
    return;
  }
  if (command.type === "abort") {
    emit({ type: "response", id: command.id, command: "abort", success: true });
    emit({ type: "agent_settled" });
    return;
  }
  if (command.type !== "prompt") return;
  prompts += 1;
  emit({ type: "response", id: command.id, command: "prompt", success: true });
  if (mode === "ui" && prompts === 1) {
    emit({ type: "extension_ui_request", id: "ui-1", method: "confirm", title: "Need input" });
    return;
  }
  if (mode === "hang") return;
  if (mode === "fail") {
    emitReport();
    process.exit(7);
  }
  if (mode === "repair" && prompts < 3) return settleWithoutReport();
  if (mode === "missing") return settleWithoutReport();
  respondWithReport();
}
function respondWithReport() {
  emit({ type: "message_update", assistantMessageEvent: { type: "text_delta", delta: "ignored cumulative stream" }, message: { role: "assistant", content: [] } });
  emitReport();
  emit({ type: "message_end", message: { role: "assistant", content: [{ type: "toolCall", name: "subagent_report" }], model: "fake-model", provider: "fake-provider", stopReason: "toolUse", usage: { input: 10, output: 4, totalTokens: 14, cost: { total: 0.01 } } } });
  emit({ type: "agent_settled" });
}
function emitReport() {
  emit({ type: "tool_execution_end", toolCallId: "report-1", toolName: "subagent_report", isError: false, result: { content: [{ type: "text", text: "accepted" }], details: { schemaVersion: 1, report: { outcome: "completed", summary: "Fake worker completed.", artifacts: [{ path: "artifact.txt" }] } }, terminate: true } });
}
function settleWithoutReport() {
  emit({ type: "message_end", message: { role: "assistant", content: [{ type: "text", text: `Fallback ${prompts}` }], model: "fake-model", provider: "fake-provider", stopReason: "stop", usage: { input: 2, output: 1, totalTokens: 3, cost: { total: 0.001 } } } });
  emit({ type: "agent_settled" });
}
