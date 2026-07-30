#!/usr/bin/env node
import { readFile } from "node:fs/promises";
import { BrainstormPrototype } from "./brainstorm-tools.mjs";

const [toolName, paramsArgument] = process.argv.slice(2);
if (!toolName || toolName === "--help") {
  console.log(`Usage:
  node cli.mjs <tool-name> '<json-params>'
  node cli.mjs <tool-name> @params.json
  echo '<json-params>' | node cli.mjs <tool-name>

Agent tools:
  dag_brainstorm_context
  dag_brainstorm_update
  dag_brainstorm_review
  dag_brainstorm_resolve_review
  dag_brainstorm_promote

Prototype harness command:
  start  # simulates the user-facing new/resume command boundary

The params object may include "root"; it defaults to the current directory.`);
  process.exit(toolName ? 0 : 1);
}

let raw = paramsArgument;
if (raw?.startsWith("@")) raw = await readFile(raw.slice(1), "utf8");
if (!raw && !process.stdin.isTTY) raw = await readStdin();
const params = raw ? JSON.parse(raw) : {};
const runtime = new BrainstormPrototype(params.root ?? process.cwd());
delete params.root;
const result = toolName === "start" ? await runtime.start(params) : await runtime.execute(toolName, params);
console.log(JSON.stringify(result, null, 2));

async function readStdin() {
  let value = "";
  for await (const chunk of process.stdin) value += chunk;
  return value;
}
