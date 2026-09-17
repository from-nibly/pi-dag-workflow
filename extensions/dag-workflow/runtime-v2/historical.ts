import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { DagPlanningStoreV1 } from "../planning/store.ts";
import { DagRunSnapshotStoreV1 } from "../dag-runtime/store.ts";
import { parseCanonicalDagPlanV1 } from "../dag-runtime/plan.ts";
import { buildSchedulerPlanIndexV1 } from "../dag-runtime/scheduler.ts";
import { parseStrictJson } from "../dag-runtime/common.ts";
import { RunEvaluationStoreV1 } from "../dag-runtime/evaluation-store.ts";
import { requireV2, IdV2, validateShapeV2 } from "../planning/v2.ts";

/** Explicit version/path dispatch. No conductor attach, lease, repair, ingest,
 * evaluation observer, migration or latest-record selection occurs here. */
export async function historicalV1(root: string, input: { kind: "plan" | "run" | "git" | "workers" | "evaluation"; id: string; revision?: number }, manager?: any) {
  requireV2(input.kind === "plan" || input.revision === undefined, "REVISION_SELECTOR_ONLY_SUPPORTED_FOR_V1_PLANS");
  if (input.kind === "evaluation") {
    requireV2(/^sha256:[0-9a-f]{64}$/.test(input.id), "EXACT_EVALUATION_HASH_REQUIRED");
    return new RunEvaluationStoreV1(root).readEnvelope(input.id);
  }
  validateShapeV2(IdV2, input.id);
  if (input.kind === "plan") return new DagPlanningStoreV1(root).read(input.id, input.revision);
  const store = new DagRunSnapshotStoreV1(join(root, ".ai", "dag-runs-v1"), input.id);
  const plan = parseCanonicalDagPlanV1(await readFile(join(store.runDirectory, "authority", "plan.json"), "utf8"));
  const context = parseStrictJson(await readFile(join(store.runDirectory, "authority", "context.json"), "utf8")) as any;
  const state = await store.read({ ...context, plan, normalizedSchedulerIndexHash: buildSchedulerPlanIndexV1(plan).indexHash });
  if (input.kind === "workers") {
    requireV2(manager, "WORKER_READER_UNAVAILABLE");
    return Promise.all(Object.values(state.workerBindings).map(binding => manager.inspectBindingReadOnly(binding)));
  }
  if (input.kind === "git") return { repositories: state.repositories, integrationTrains: state.integrationTrains, integrationAttempts: state.integrationAttempts, evidenceIndex: state.evidenceIndex };
  return state;
}
