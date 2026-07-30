import { appendFile, readFile } from "node:fs/promises";
import { join } from "node:path";

const args = process.argv.slice(2);
const command = args[0] === "poll" || args[0] === "end" ? args[0] : "open";
const file = command === "open" ? args[0] : args[1];
if (process.env.FAKE_LAVISH_LOG) await appendFile(process.env.FAKE_LAVISH_LOG, `${JSON.stringify({ command, file, args })}\n`);

if (command === "poll") {
  const mode = process.env.FAKE_LAVISH_POLL ?? "feedback";
  if (mode === "wait") {
    await new Promise((resolve) => setTimeout(resolve, 30_000));
    process.stdout.write(`session:\n  file: ${file}\n  status: waiting\n`);
  } else {
    const fixture = mode === "ended" ? "poll-ended.toon" : mode === "layout" ? "poll-layout.toon" : "poll-feedback.toon";
    process.stdout.write(await readFile(join(process.env.FAKE_LAVISH_FIXTURES, fixture), "utf8"));
  }
} else if (command === "end") {
  process.stdout.write(`session:\n  file: ${file}\n  status: ended\n  ended_by: agent\n`);
} else {
  process.stdout.write(`session:\n  file: ${file}\n  url: \"http://127.0.0.1:4387/session/fake\"\n  status: opened\n`);
}
