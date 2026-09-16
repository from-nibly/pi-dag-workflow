import { Type, type Static } from "typebox";
import { StrictObject, HashSchema, GitOidSchema, canonicalHash, canonicalStringify, schemaIssues, parseStrictJson } from "../dag-runtime/common.ts";

export const IdV2 = Type.String({ pattern: "^(?!constructor$|prototype$|__proto__$)[A-Za-z0-9][A-Za-z0-9._-]{0,127}$", maxLength: 128 });
export const TextV2 = Type.String({ minLength: 1, maxLength: 65536 });
export const CountV2 = Type.Integer({ minimum: 1, maximum: 1000000 });
const strings = Type.Array(TextV2, { maxItems: 512 });
const ids = Type.Array(IdV2, { maxItems: 512, uniqueItems: true });
export const PlanSelectorV2Schema = StrictObject({ planId: IdV2, revision: CountV2, planHash: HashSchema });
export const RepositoryV2Schema = StrictObject({ repositoryId: IdV2, baselineCommit: GitOidSchema, baselineTree: GitOidSchema, targetBranch: TextV2 });
const command = StrictObject({ id: IdV2, argv: Type.Array(TextV2, { minItems: 1, maxItems: 128 }) });
export const PlanV2Schema = StrictObject({
  kind: Type.Literal("dag_plan_v2"), schemaVersion: Type.Literal(2),
  planId: IdV2, revision: CountV2, planHash: HashSchema,
  predecessor: Type.Optional(PlanSelectorV2Schema), title: TextV2,
  repository: RepositoryV2Schema,
  source: StrictObject({ governingClosure: HashSchema, refs: Type.Array(StrictObject({ ref: TextV2, digest: HashSchema }), { maxItems: 512 }), scopeSummary: TextV2 }),
  architecture: StrictObject({ outcomes: Type.Array(StrictObject({ id: IdV2, description: TextV2 }), { minItems: 1, maxItems: 512 }), nonGoals: strings, notes: strings, risks: strings }),
  workItems: Type.Array(StrictObject({ id: IdV2, title: TextV2, objective: TextV2, outcomeIds: ids, context: strings, checks: Type.Array(TextV2, { minItems: 1, maxItems: 512 }), dependsOn: ids,
    risk: Type.Union([Type.Literal("low"), Type.Literal("medium"), Type.Literal("high")]), riskNotes: strings,
    resources: Type.Record(IdV2, CountV2), gates: ids,
  }), { minItems: 1, maxItems: 512 }),
  constraints: StrictObject({ maxConcurrency: CountV2, resources: Type.Record(IdV2, CountV2), mutexGroups: Type.Array(StrictObject({ id: IdV2, workItemIds: ids, reason: TextV2 }), { maxItems: 512 }), gates: ids }),
  integration: StrictObject({ strategy: Type.Union([Type.Literal("dependency_order"), Type.Literal("serial")]), checks: Type.Array(TextV2, { minItems: 1, maxItems: 512 }), finalChecks: Type.Array(TextV2, { minItems: 1, maxItems: 512 }), prefixCommands: Type.Array(command, { minItems: 1, maxItems: 512 }), finalCommands: Type.Array(command, { minItems: 1, maxItems: 512 }) }),
});
export type PlanV2 = Static<typeof PlanV2Schema>;
export type PlanSelectorV2 = Static<typeof PlanSelectorV2Schema>;
export type PlanInputV2 = Omit<PlanV2, "kind" | "schemaVersion" | "revision" | "planHash">;
export function requireV2(condition: unknown, message: string): asserts condition { if (!condition) throw new Error(message); }
export function validateShapeV2(schema: Parameters<typeof schemaIssues>[0], value: unknown): void {
  const issues = schemaIssues(schema, value); requireV2(!issues.length, `INVALID_V2: ${issues.slice(0, 8).map(i => `${i.path}: ${i.message}`).join("; ")}`);
}
export function selectorV2(plan: PlanV2): PlanSelectorV2 { return { planId: plan.planId, revision: plan.revision, planHash: plan.planHash }; }
export function sameV2(a: unknown, b: unknown): boolean { return canonicalStringify(a) === canonicalStringify(b); }
export function planHashV2(plan: Omit<PlanV2, "planHash"> | PlanV2): string { const { planHash: _, ...content } = plan as PlanV2; return canonicalHash(content); }
export function parsePlanV2(value: unknown): PlanV2 {
  validateShapeV2(PlanV2Schema, value);
  const plan = value as PlanV2;
  requireV2(planHashV2(plan) === plan.planHash, "PLAN_HASH_MISMATCH");
  const unique = (values: string[]) => requireV2(new Set(values).size === values.length, "DUPLICATE_ID");
  unique(plan.workItems.map(n => n.id)); unique(plan.architecture.outcomes.map(o => o.id)); unique(plan.constraints.mutexGroups.map(m => m.id));
  const nodes = new Map(plan.workItems.map(n => [n.id, n]));
  const outcomes = new Set(plan.architecture.outcomes.map(o => o.id));
  for (const n of nodes.values()) {
    requireV2(n.outcomeIds.length && n.outcomeIds.every(id => outcomes.has(id)), "UNKNOWN_OUTCOME");
    requireV2(n.dependsOn.every(id => nodes.has(id) && id !== n.id), "UNKNOWN_DEPENDENCY");
    requireV2(n.gates.every(id => plan.constraints.gates.includes(id)), "UNKNOWN_GATE");
    for (const [id, demand] of Object.entries(n.resources)) requireV2(demand <= (plan.constraints.resources[id] ?? 0), "IMPOSSIBLE_RESOURCE_DEMAND");
  }
  for (const m of plan.constraints.mutexGroups) requireV2(m.workItemIds.length >= 2 && m.workItemIds.every(id => nodes.has(id)), "INVALID_MUTEX");
  const visiting = new Set<string>(), visited = new Set<string>();
  const visit = (id: string) => { requireV2(!visiting.has(id), "DEPENDENCY_CYCLE"); if (visited.has(id)) return; visiting.add(id); for (const dep of nodes.get(id)!.dependsOn) visit(dep); visiting.delete(id); visited.add(id); };
  for (const id of nodes.keys()) visit(id);
  return plan;
}
export function createPlanV2(input: PlanInputV2, revision: number): PlanV2 {
  const value = { ...structuredClone(input), kind: "dag_plan_v2" as const, schemaVersion: 2 as const, revision };
  return parsePlanV2({ ...value, planHash: planHashV2(value) });
}
export function renderPlanV2(plan: PlanV2): string {
  parsePlanV2(plan);
  return [`# ${plan.title}`, `Plan ${plan.planId} revision ${plan.revision} (${plan.planHash})`, plan.source.scopeSummary,
    ...plan.workItems.map(n => `## ${n.id}: ${n.title}\n${n.objective}\nDepends on: ${n.dependsOn.join(", ") || "none"}\n${n.checks.map(c => `- ${c}`).join("\n")}`)].join("\n\n");
}
/** Inspection never creates a V2 store or rewrites historical bytes. */
export async function inspectPlanFileV2(path: string): Promise<{ version: 1 | 2; value: unknown; raw: string }> {
  const { readFile } = await import("node:fs/promises");
  const raw = await readFile(path, "utf8"), value = parseStrictJson(raw) as any;
  if (value?.schemaVersion === 2 && value.kind === "dag_plan_v2") return { version: 2, value: parsePlanV2(value), raw };
  if (value?.schemaVersion === 1 && value.kind === "dag_planning_record") {
    const { parseDagPlanningPlanV1 } = await import("./artifact.ts");
    return { version: 1, value: parseDagPlanningPlanV1(raw), raw };
  }
  throw new Error("UNSUPPORTED_PLAN_VERSION");
}
