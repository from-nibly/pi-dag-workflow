import { readFile } from "node:fs/promises";
import { DurableGitIntegrationRuntimeV1, ExactGitIntegrationV1 } from "../../extensions/dag-workflow/dag-runtime/index.ts";

const [requestPath, failpoint, journalRoot] = process.argv.slice(2);
if (!requestPath || !failpoint) throw new Error("Usage: git-integration-crash-child.mjs <request.json> <failpoint> [journal-root]");
const request = JSON.parse(await readFile(requestPath, "utf8"));
const delegate = {
  async assertAuthority() {},
  async acceptReceipt() {},
  async verify() { return { prefixEvidenceHashes: [`sha256:${"a".repeat(64)}`], finalEvidenceHashes: [`sha256:${"b".repeat(64)}`], environmentClosureHash: `sha256:${"c".repeat(64)}` }; },
  async failpoint(point) { if (point === failpoint) process.exit(86); },
  now: () => request.planCreatedAt,
};
const runtime = journalRoot ? new DurableGitIntegrationRuntimeV1(journalRoot, delegate) : { ...delegate, async recordIntent() {}, async recordObservation() {} };
await new ExactGitIntegrationV1(runtime).execute(request);
throw new Error(`Failpoint was not reached: ${failpoint}`);
