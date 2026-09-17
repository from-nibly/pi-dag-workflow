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
export const StageV2Schema = Type.Union(([1, 2, 3, 4, 5, 6, 7] as const).map(n => Type.Literal(n)));
export const ProcedureV2Schema = Type.Union([
  StrictObject({ kind: Type.Literal("command"), argv: Type.Array(TextV2, { minItems: 1, maxItems: 128 }) }),
  StrictObject({ kind: Type.Literal("producer"), producerId: IdV2 }),
]);
export const LifecycleCheckV2Schema = StrictObject({ id: IdV2, stage: StageV2Schema, expectation: TextV2,
  sourceRefs: Type.Array(TextV2, { minItems: 1, maxItems: 32 }),
  applicability: Type.Union([StrictObject({ kind: Type.Literal("required") }),
    StrictObject({ kind: Type.Literal("not_applicable"), reason: TextV2, evidence: Type.Array(TextV2, { minItems: 1, maxItems: 32 }) })]),
  procedure: ProcedureV2Schema, environment: TextV2,
  replay: Type.Union([Type.Literal("pure"), Type.Literal("idempotent"), Type.Literal("non_repeatable")]),
});
export const LifecyclePlanV2Schema = StrictObject({
  oracle: StrictObject({ statement: TextV2, sourceRefs: Type.Array(TextV2, { minItems: 1, maxItems: 32 }), checkIds: Type.Array(IdV2, { minItems: 1, maxItems: 128, uniqueItems: true }) }),
  checks: Type.Array(LifecycleCheckV2Schema, { minItems: 7, maxItems: 128 }),
});
export type LifecycleCheckV2 = Static<typeof LifecycleCheckV2Schema>;
export const SourceScopeV2Schema = StrictObject({ kind: Type.Literal("model_scope_v2"), focusId: IdV2, workstreamIds: ids });
export const PlanInputV2Schema = StrictObject({
  planId: IdV2,
  predecessor: Type.Optional(PlanSelectorV2Schema), title: TextV2,
  repository: RepositoryV2Schema,
  source: StrictObject({ selector: Type.Optional(SourceScopeV2Schema), governingClosure: HashSchema, refs: Type.Array(StrictObject({ ref: TextV2, digest: HashSchema }), { maxItems: 512 }), scopeSummary: TextV2 }),
  architecture: StrictObject({ outcomes: Type.Array(StrictObject({ id: IdV2, description: TextV2 }), { minItems: 1, maxItems: 512 }), nonGoals: strings, notes: strings, risks: strings }),
  workItems: Type.Array(StrictObject({ id: IdV2, title: TextV2, objective: TextV2, outcomeIds: ids, context: strings, checks: Type.Array(TextV2, { minItems: 1, maxItems: 512 }), dependsOn: ids,
    risk: Type.Union([Type.Literal("low"), Type.Literal("medium"), Type.Literal("high")]), riskNotes: strings,
    resources: Type.Record(IdV2, CountV2, { additionalProperties: false }), gates: ids, lifecycle: LifecyclePlanV2Schema,
  }), { minItems: 1, maxItems: 512 }),
  constraints: StrictObject({ maxConcurrency: CountV2, resources: Type.Record(IdV2, CountV2, { additionalProperties: false }), mutexGroups: Type.Array(StrictObject({ id: IdV2, workItemIds: ids, reason: TextV2 }), { maxItems: 512 }), gates: ids }),
  integration: StrictObject({ strategy: Type.Union([Type.Literal("dependency_order"), Type.Literal("serial")]), checks: Type.Array(TextV2, { minItems: 1, maxItems: 512 }), finalChecks: Type.Array(TextV2, { minItems: 1, maxItems: 512 }), prefixCommands: Type.Array(command, { minItems: 1, maxItems: 512 }), finalCommands: Type.Array(command, { minItems: 1, maxItems: 512 }) }),
});
export const PlanV2Schema = StrictObject({
  ...PlanInputV2Schema.properties,
  kind: Type.Literal("dag_plan_v2"), schemaVersion: Type.Literal(2), revision: CountV2, planHash: HashSchema,
});
export type PlanV2 = Static<typeof PlanV2Schema>;
export type PlanSelectorV2 = Static<typeof PlanSelectorV2Schema>;
export type PlanInputV2 = Static<typeof PlanInputV2Schema>;
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
  const sourceRefs = new Set(plan.source.refs.map(r => r.ref));
  unique(plan.source.refs.map(r => r.ref));
  for (const n of nodes.values()) {
    unique(n.lifecycle.checks.map(c => c.id));
    requireV2(n.lifecycle.oracle.sourceRefs.every(ref => sourceRefs.has(ref))
      && n.lifecycle.checks.every(c => c.sourceRefs.every(ref => sourceRefs.has(ref))
        && (c.applicability.kind !== "not_applicable" || c.applicability.evidence.every(ref => sourceRefs.has(ref)))), "UNGROUNDED_LIFECYCLE_REFERENCE");
    for (let stage = 1; stage <= 7; stage++) requireV2(n.lifecycle.checks.some(c => c.stage === stage && c.applicability.kind === "required"), "MISSING_LIFECYCLE_STAGE_CHECK");
    requireV2(n.lifecycle.oracle.checkIds.every(id => n.lifecycle.checks.some(c => c.id === id && c.stage === 2 && c.applicability.kind === "required")), "ORACLE_NOT_EVALUATED_AT_F2");
    // This initial profile replays every applicable check at F7. Non-repeatable
    // checks need a validity-window/waiver design before they can be supported.
    requireV2(n.lifecycle.checks.every(c => c.applicability.kind !== "required" || c.replay !== "non_repeatable"), "NON_REPLAYABLE_LIFECYCLE_CHECK");
    requireV2(n.outcomeIds.length && n.outcomeIds.every(id => outcomes.has(id)), "UNKNOWN_OUTCOME");
    requireV2(n.dependsOn.every(id => nodes.has(id) && id !== n.id), "UNKNOWN_DEPENDENCY");
    requireV2(n.gates.every(id => plan.constraints.gates.includes(id)), "UNKNOWN_GATE");
    for (const [id, demand] of Object.entries(n.resources)) requireV2(demand <= (plan.constraints.resources[id] ?? 0), "IMPOSSIBLE_RESOURCE_DEMAND");
  }
  for (const m of plan.constraints.mutexGroups) requireV2(m.workItemIds.length >= 2 && m.workItemIds.every(id => nodes.has(id)), "INVALID_MUTEX");
  const visiting = new Set<string>(), visited = new Set<string>();
  const visit = (id: string) => { requireV2(!visiting.has(id), "DEPENDENCY_CYCLE"); if (visited.has(id)) return; visiting.add(id); for (const dep of nodes.get(id)!.dependsOn) visit(dep); visiting.delete(id); visited.add(id); };
  for (const id of nodes.keys()) visit(id);
  if (plan.integration.strategy === "serial") {
    const earlier = new Set<string>();
    for (const n of plan.workItems) {
      requireV2(n.dependsOn.every(id => earlier.has(id)), "SERIAL_DEPENDENCY_ORDER");
      earlier.add(n.id);
    }
  }
  return plan;
}
export function createPlanV2(input: PlanInputV2, revision: number): PlanV2 {
  validateShapeV2(PlanInputV2Schema, input);
  const value = { ...structuredClone(input), kind: "dag_plan_v2" as const, schemaVersion: 2 as const, revision };
  return parsePlanV2({ ...value, planHash: planHashV2(value) });
}
export function renderPlanV2(plan: PlanV2): string {
  parsePlanV2(plan);
  return [`# ${plan.title}`, `Plan ${plan.planId} revision ${plan.revision} (${plan.planHash})`, plan.source.scopeSummary,
    `## Architecture\n${plan.architecture.outcomes.map(o => `- ${o.id}: ${o.description}`).join("\n")}\nNon-goals: ${plan.architecture.nonGoals.join("; ") || "none"}\nNotes: ${plan.architecture.notes.join("; ")}\nRisks: ${plan.architecture.risks.join("; ")}`,
    `## Frozen sources\n${JSON.stringify(plan.source.selector ?? null)}\n${plan.source.refs.map(r => `- ${r.ref}: ${r.digest}`).join("\n")}`,
    `## Constraints\n${JSON.stringify(plan.constraints)}`,
    ...plan.workItems.map(n => `## ${n.id}: ${n.title}\n${n.objective}\nOutcomes: ${n.outcomeIds.join(", ")}\nDepends on: ${n.dependsOn.join(", ") || "none"}\nContext: ${n.context.join("; ")}\nRisk: ${n.risk}; ${n.riskNotes.join("; ")}\nResources: ${JSON.stringify(n.resources)}; gates: ${n.gates.join(", ") || "none"}\nOracle: ${n.lifecycle.oracle.statement}\n${n.lifecycle.checks.map(c => `- F${c.stage} ${c.id}: ${c.expectation} (${c.applicability.kind}); ${JSON.stringify(c.procedure)}`).join("\n")}\n${n.checks.map(c => `- ${c}`).join("\n")}`),
    `## Integration\n${JSON.stringify(plan.integration, null, 2)}`,
    `## Lineage\n${plan.predecessor ? JSON.stringify(plan.predecessor) : "No predecessor"}`].join("\n\n");
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
