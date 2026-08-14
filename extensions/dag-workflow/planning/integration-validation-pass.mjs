#!/usr/bin/env node

import { spawnSync } from "node:child_process";

let disposition = "PASS";
try {
  const [mode, phase, encoded] = process.argv.slice(2);
  if (mode !== "--commands" || !["prefix", "final"].includes(phase) || !encoded) throw new Error("invalid validation invocation");
  const commands = JSON.parse(encoded);
  if (!Array.isArray(commands) || commands.length < 1 || commands.length > 16) throw new Error("invalid validation command set");
  const ids = new Set();
  for (const command of commands) {
    if (!command || Object.keys(command).sort().join(",") !== "argv,id" || typeof command.id !== "string" || !Array.isArray(command.argv) || command.argv.length < 1 || command.argv.some((value) => typeof value !== "string" || !value || value.includes("\0")) || ids.has(command.id)) throw new Error("invalid validation command");
    ids.add(command.id);
    const result = spawnSync(command.argv[0], command.argv.slice(1), { cwd: process.cwd(), env: process.env, shell: false, stdio: "ignore", timeout: 60_000 });
    if (result.error || result.status !== 0 || result.signal) { disposition = "FAIL"; break; }
  }
  if (disposition === "PASS") {
    const clean = spawnSync("git", ["status", "--porcelain=v2", "--untracked-files=all"], { cwd: process.cwd(), env: process.env, shell: false, encoding: "utf8", timeout: 30_000 });
    const tree = spawnSync("git", ["diff-tree", "--check", "--root", "HEAD"], { cwd: process.cwd(), env: process.env, shell: false, stdio: "ignore", timeout: 30_000 });
    if (clean.error || clean.status !== 0 || clean.signal || clean.stdout.trim() || tree.error || tree.status !== 0 || tree.signal) disposition = "FAIL";
  }
} catch {
  disposition = "FAIL";
}
process.stdout.write(JSON.stringify({ disposition }));
