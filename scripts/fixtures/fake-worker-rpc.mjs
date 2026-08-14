#!/usr/bin/env node
import { spawn } from "node:child_process";
import { StringDecoder } from "node:string_decoder";

const mode = process.env.FAKE_WORKER_RPC_MODE ?? "valid";
const decoder = new StringDecoder("utf8");
let buffer = "";
let prompts = 0;
if (mode === "forced-after-report") setInterval(() => {}, 1000);
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
process.stdin.on("end", () => { if (mode !== "forced-after-report") process.exit(0); });

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
  if (mode === "detached-grandchild") {
    const grandchild = spawn(process.execPath, ["-e", "const fs=require('node:fs');const p=process.argv[1];const t=setInterval(()=>fs.appendFileSync(p,'x'),50);setTimeout(()=>{clearInterval(t);process.exit(0)},2000)", "detached-grandchild-writes.txt"], { cwd: process.cwd(), detached: true, stdio: "ignore" });
    grandchild.unref();
  }
  if (mode === "detached-uninspectable") {
    const code = "import ctypes,time\nlibc=ctypes.CDLL(None)\nlibc.prctl(4,0,0,0,0)\nend=time.time()+5\nwhile time.time()<end:\n open('uninspectable-descendant-writes.txt','a').write('x')\n time.sleep(.05)";
    const grandchild = spawn("python3", ["-c", code], { cwd: process.cwd(), detached: true, stdio: "ignore" });
    grandchild.unref();
  }
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
