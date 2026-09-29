import { writeImmutableJson, writeImmutableBytes } from "../../extensions/dag-workflow/worker-runtime/core.mjs";
const [kind, path, value] = process.argv.slice(2);
const published = kind === "json" ? await writeImmutableJson(path, JSON.parse(value)) : await writeImmutableBytes(path, Buffer.from(value));
console.log(JSON.stringify({ published }));
