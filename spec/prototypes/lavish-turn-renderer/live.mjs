import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { LavishCliAdapter } from "./lavish-cli.mjs";
import { LavishTurnLifecycle } from "./lifecycle.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const projection = JSON.parse(await readFile(resolve(here, "fixtures/whole-turn.json"), "utf8"));
const root = resolve(here, "../../../..");
const cli = new LavishCliAdapter({ command: "npx", argsPrefix: ["-y", "lavish-axi"] });
const lifecycle = new LavishTurnLifecycle({ root, cli });
const args = new Set(process.argv.slice(2));

if (args.has("--render-only")) {
  const result = await lifecycle.render(projection);
  console.log(JSON.stringify({ status: result.metadata.status, artifactPath: result.paths.html }, null, 2));
  process.exit(0);
}

const controller = new AbortController();
process.once("SIGINT", () => controller.abort());
process.once("SIGTERM", () => controller.abort());
try {
  const update = (event) => console.error(`[lavish prototype] ${event.phase}: ${event.artifactPath ?? ""}`);
  const result = args.has("--resume")
    ? await lifecycle.resume(projection, { signal: controller.signal, onUpdate: update, reopen: args.has("--reopen") })
    : await lifecycle.present(projection, { signal: controller.signal, onUpdate: update, noOpen: args.has("--no-open") });
  console.log(JSON.stringify(result.feedback, null, 2));
} catch (error) {
  if (error?.name === "AbortError" || controller.signal.aborted) {
    const paths = lifecycle.paths(projection);
    console.error(`Polling interrupted; artifact remains resumable at ${paths.html}`);
    process.exitCode = 130;
  } else throw error;
}
