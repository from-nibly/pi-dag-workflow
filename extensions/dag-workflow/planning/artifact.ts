import { Type, type Static } from "typebox";
import {
  BoundedTextSchema,
  GitOidSchema,
  HashSchema,
  PositiveIntegerSchema,
  StrictObject,
  TimestampSchema,
  assertValid,
  canonicalHash,
  parseStrictJson,
  pushIssue,
  schemaIssues,
  validateTimestampFields,
  type ValidationIssue,
  type ValidationResult,
} from "../dag-runtime/common.ts";
import {
  DAG_PLANNING_SCHEMA_VERSION,
  type DagPlanningPlanInputV1,
  type DagPlanningPlanV1,
} from "./types.ts";

const PlanningIdSchema = Type.String({ minLength: 1, maxLength: 128, pattern: "^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$" });
const RequiredTextSchema = Type.String({ minLength: 1, maxLength: 65_536 });
const ShortTextSchema = Type.String({ minLength: 1, maxLength: 512 });
const TextListSchema = Type.Array(RequiredTextSchema, { maxItems: 512 });
const NullableTextSchema = Type.Union([BoundedTextSchema, Type.Null()]);
const NullableTimestampSchema = Type.Union([TimestampSchema, Type.Null()]);
const OptionalSummarySchema = Type.Optional(ShortTextSchema);
const ModelCollectionSchema = Type.Union([
  Type.Literal("workstreams"),
  Type.Literal("intents"),
  Type.Literal("concepts"),
  Type.Literal("evidence"),
  Type.Literal("assumptions"),
  Type.Literal("questions"),
  Type.Literal("tensions"),
  Type.Literal("scenarios"),
  Type.Literal("proposals"),
  Type.Literal("decisions"),
  Type.Literal("commitments"),
  Type.Literal("discoveries"),
]);

const SourceRefSchema = Type.Union([
  StrictObject({
    kind: Type.Literal("project_model_object"),
    collection: ModelCollectionSchema,
    objectId: Type.String({ minLength: 1, maxLength: 512 }),
    semanticHash: HashSchema,
    summary: OptionalSummarySchema,
  }),
  StrictObject({
    kind: Type.Literal("generated_spec"),
    path: Type.String({ minLength: 1, maxLength: 4_096 }),
    contentHash: HashSchema,
    summary: OptionalSummarySchema,
  }),
  StrictObject({
    kind: Type.Literal("external"),
    ref: Type.String({ minLength: 1, maxLength: 4_096 }),
    summary: OptionalSummarySchema,
  }),
]);
const OutcomeSchema = StrictObject({ id: PlanningIdSchema, description: RequiredTextSchema });
const RepositorySchema = StrictObject({
  repositoryId: PlanningIdSchema,
  baselineCommit: GitOidSchema,
  baselineTree: GitOidSchema,
  targetBranch: Type.String({ minLength: 1, maxLength: 255, pattern: "^[^\\s\\x00-\\x1f\\x7f]+$" }),
});
const WorkItemSchema = StrictObject({
  id: PlanningIdSchema,
  title: ShortTextSchema,
  objective: RequiredTextSchema,
  outcomeIds: Type.Array(PlanningIdSchema, { minItems: 1, maxItems: 256 }),
  context: TextListSchema,
  checks: Type.Array(RequiredTextSchema, { minItems: 1, maxItems: 256 }),
  dependsOn: Type.Array(PlanningIdSchema, { maxItems: 256 }),
  risk: Type.Union([Type.Literal("low"), Type.Literal("medium"), Type.Literal("high")]),
  riskNotes: TextListSchema,
  constraints: Type.Optional(TextListSchema),
});
const MutexGroupSchema = StrictObject({
  id: PlanningIdSchema,
  workItemIds: Type.Array(PlanningIdSchema, { minItems: 2, maxItems: 256 }),
  reason: RequiredTextSchema,
});
const ApprovalSchema = StrictObject({
  status: Type.Union([Type.Literal("pending"), Type.Literal("approved"), Type.Literal("rejected")]),
  by: NullableTextSchema,
  at: NullableTimestampSchema,
  note: NullableTextSchema,
});
const ValidationCommandSchema = StrictObject({
  id: PlanningIdSchema,
  argv: Type.Array(Type.String({ minLength: 1, maxLength: 1_024 }), { minItems: 1, maxItems: 32 }),
});
const AuthorizationSchema = StrictObject({
  status: Type.Union([Type.Literal("not_authorized"), Type.Literal("authorized"), Type.Literal("revoked")]),
  by: NullableTextSchema,
  at: NullableTimestampSchema,
  scope: TextListSchema,
  maxConcurrency: Type.Union([PositiveIntegerSchema, Type.Null()]),
  note: NullableTextSchema,
});

export const DagPlanningPlanV1Schema = StrictObject({
  schemaVersion: Type.Literal(DAG_PLANNING_SCHEMA_VERSION),
  kind: Type.Literal("dag_planning_record"),
  planId: PlanningIdSchema,
  revision: PositiveIntegerSchema,
  status: Type.Union([Type.Literal("draft"), Type.Literal("ready"), Type.Literal("superseded")]),
  title: ShortTextSchema,
  focusId: Type.Union([PlanningIdSchema, Type.Null()]),
  createdAt: TimestampSchema,
  updatedAt: TimestampSchema,
  repository: RepositorySchema,
  source: StrictObject({
    refs: Type.Array(SourceRefSchema, { maxItems: 256 }),
    scopeSummary: RequiredTextSchema,
  }),
  architecture: StrictObject({
    outcomes: Type.Array(OutcomeSchema, { minItems: 1, maxItems: 256 }),
    nonGoals: TextListSchema,
    notes: TextListSchema,
    risks: TextListSchema,
  }),
  workItems: Type.Array(WorkItemSchema, { minItems: 1, maxItems: 1_024 }),
  constraints: StrictObject({
    maxConcurrency: Type.Union([PositiveIntegerSchema, Type.Null()]),
    mutexGroups: Type.Array(MutexGroupSchema, { maxItems: 256 }),
  }),
  integration: StrictObject({
    strategy: Type.Union([Type.Literal("dependency_order"), Type.Literal("serial")]),
    checks: Type.Array(RequiredTextSchema, { minItems: 1, maxItems: 256 }),
    finalChecks: Type.Array(RequiredTextSchema, { minItems: 1, maxItems: 256 }),
    prefixCommands: Type.Array(ValidationCommandSchema, { minItems: 1, maxItems: 16 }),
    finalCommands: Type.Array(ValidationCommandSchema, { minItems: 1, maxItems: 16 }),
  }),
  approval: ApprovalSchema,
  authorization: AuthorizationSchema,
  planHash: HashSchema,
});

export type DagPlanningPlanSchemaV1 = Static<typeof DagPlanningPlanV1Schema>;

export function dagPlanningStaticContentV1(plan: Omit<DagPlanningPlanV1, "planHash"> | DagPlanningPlanV1): Record<string, unknown> {
  return structuredClone({
    schemaVersion: plan.schemaVersion,
    kind: plan.kind,
    planId: plan.planId,
    title: plan.title,
    focusId: plan.focusId,
    repository: plan.repository,
    source: plan.source,
    architecture: plan.architecture,
    workItems: plan.workItems,
    constraints: plan.constraints,
    integration: plan.integration,
  });
}

export function dagPlanningPlanHashV1(plan: Omit<DagPlanningPlanV1, "planHash"> | DagPlanningPlanV1): string {
  return canonicalHash(dagPlanningStaticContentV1(plan));
}

export function sealDagPlanningPlanV1(plan: Omit<DagPlanningPlanV1, "planHash">): DagPlanningPlanV1 {
  const sealed = structuredClone({ ...plan, planHash: dagPlanningPlanHashV1(plan) }) as DagPlanningPlanV1;
  assertDagPlanningPlanV1(sealed);
  return sealed;
}

export function createDagPlanningPlanV1(input: DagPlanningPlanInputV1, now = new Date().toISOString()): DagPlanningPlanV1 {
  return sealDagPlanningPlanV1({
    ...structuredClone(input),
    schemaVersion: DAG_PLANNING_SCHEMA_VERSION,
    kind: "dag_planning_record",
    revision: 1,
    createdAt: now,
    updatedAt: now,
  });
}

export function validateDagPlanningPlanV1(value: unknown): ValidationResult<DagPlanningPlanV1> {
  const issues = schemaIssues(DagPlanningPlanV1Schema, value);
  if (issues.length > 0) return { ok: false, issues };
  const plan = value as DagPlanningPlanV1;
  validateTimestampFields(plan, issues);
  pushIssue(issues, "/planHash", plan.planHash === dagPlanningPlanHashV1(plan), "does not match canonical static plan content");
  validatePlanSemantics(plan, issues);
  return issues.length > 0 ? { ok: false, issues } : { ok: true, value: plan, issues };
}

export function assertDagPlanningPlanV1(value: unknown): asserts value is DagPlanningPlanV1 {
  assertValid(validateDagPlanningPlanV1(value), "Invalid DAG planning record");
}

export function parseDagPlanningPlanV1(text: string): DagPlanningPlanV1 {
  const value = parseStrictJson(text);
  assertDagPlanningPlanV1(value);
  return value;
}

function validatePlanSemantics(plan: DagPlanningPlanV1, issues: ValidationIssue[]): void {
  pushIssue(issues, "/updatedAt", Date.parse(plan.updatedAt) >= Date.parse(plan.createdAt), "must not precede createdAt");

  const outcomeIds = uniqueIds(plan.architecture.outcomes, "/architecture/outcomes", issues);
  const workItemIds = uniqueIds(plan.workItems, "/workItems", issues);
  uniqueIds(plan.constraints.mutexGroups, "/constraints/mutexGroups", issues);
  uniqueStrings(plan.source.refs.map(sourceRefIdentity), "/source/refs", issues);
  for (const [index, ref] of plan.source.refs.entries()) {
    if (ref.kind === "generated_spec") {
      pushIssue(issues, `/source/refs/${index}/path`, isRepositoryRelativePath(ref.path), "must be a normalized repository-relative path");
    }
  }
  uniqueStrings(plan.architecture.nonGoals, "/architecture/nonGoals", issues);
  uniqueStrings(plan.integration.checks, "/integration/checks", issues);
  uniqueStrings(plan.integration.finalChecks, "/integration/finalChecks", issues);
  uniqueIds(plan.integration.prefixCommands, "/integration/prefixCommands", issues);
  uniqueIds(plan.integration.finalCommands, "/integration/finalCommands", issues);
  validateValidationCommands(plan.integration.prefixCommands, "/integration/prefixCommands", issues);
  validateValidationCommands(plan.integration.finalCommands, "/integration/finalCommands", issues);
  uniqueStrings(plan.authorization.scope, "/authorization/scope", issues);

  for (const [index, item] of plan.workItems.entries()) {
    const path = `/workItems/${index}`;
    uniqueStrings(item.outcomeIds, `${path}/outcomeIds`, issues);
    uniqueStrings(item.context, `${path}/context`, issues);
    uniqueStrings(item.checks, `${path}/checks`, issues);
    uniqueStrings(item.dependsOn, `${path}/dependsOn`, issues);
    uniqueStrings(item.riskNotes, `${path}/riskNotes`, issues);
    if (item.constraints) uniqueStrings(item.constraints, `${path}/constraints`, issues);
    for (const id of item.outcomeIds) pushIssue(issues, `${path}/outcomeIds`, outcomeIds.has(id), `references missing outcome ${id}`);
    for (const id of item.dependsOn) {
      pushIssue(issues, `${path}/dependsOn`, workItemIds.has(id), `references missing work item ${id}`);
      pushIssue(issues, `${path}/dependsOn`, id !== item.id, "must not contain the work item's own ID");
    }
  }

  for (const [index, group] of plan.constraints.mutexGroups.entries()) {
    const path = `/constraints/mutexGroups/${index}/workItemIds`;
    uniqueStrings(group.workItemIds, path, issues);
    for (const id of group.workItemIds) pushIssue(issues, path, workItemIds.has(id), `references missing work item ${id}`);
  }

  validateAcyclic(plan, issues);
  validateApproval(plan, issues);
  validateAuthorization(plan, issues);
  if (plan.constraints.maxConcurrency !== null) {
    pushIssue(issues, "/constraints/maxConcurrency", plan.constraints.maxConcurrency <= plan.workItems.length, "must not exceed the number of work items");
  }
  if (plan.authorization.maxConcurrency !== null && plan.constraints.maxConcurrency !== null) {
    pushIssue(issues, "/authorization/maxConcurrency", plan.authorization.maxConcurrency <= plan.constraints.maxConcurrency, "must not exceed the plan constraint");
  }
}

function validateApproval(plan: DagPlanningPlanV1, issues: ValidationIssue[]): void {
  const decided = plan.approval.status !== "pending";
  pushIssue(issues, "/approval/by", decided ? isNonempty(plan.approval.by) : plan.approval.by === null, decided ? "is required for a decided approval" : "must be null while pending");
  pushIssue(issues, "/approval/at", decided ? plan.approval.at !== null : plan.approval.at === null, decided ? "is required for a decided approval" : "must be null while pending");
  if (plan.status === "ready") pushIssue(issues, "/approval/status", plan.approval.status === "approved", "must be approved when the plan is ready");
}

function validateAuthorization(plan: DagPlanningPlanV1, issues: ValidationIssue[]): void {
  const decided = plan.authorization.status !== "not_authorized";
  pushIssue(issues, "/authorization/by", decided ? isNonempty(plan.authorization.by) : plan.authorization.by === null, decided ? "is required for an authorization decision" : "must be null while not authorized");
  pushIssue(issues, "/authorization/at", decided ? plan.authorization.at !== null : plan.authorization.at === null, decided ? "is required for an authorization decision" : "must be null while not authorized");
  if (plan.authorization.status === "authorized") {
    const authorizedWorkItems = [...plan.authorization.scope].sort();
    const plannedWorkItems = plan.workItems.map(({ id }) => id).sort();
    pushIssue(issues, "/authorization/status", plan.status === "ready" && plan.approval.status === "approved", "requires a ready approved plan");
    pushIssue(issues, "/authorization/scope", JSON.stringify(authorizedWorkItems) === JSON.stringify(plannedWorkItems), "must authorize every exact work-item ID once for the whole-plan V1 runtime");
    pushIssue(issues, "/authorization/maxConcurrency", plan.authorization.maxConcurrency !== null, "is required when authorized");
  }
}

function validateAcyclic(plan: DagPlanningPlanV1, issues: ValidationIssue[]): void {
  const state = new Map<string, 0 | 1 | 2>();
  const stack: string[] = [];
  const byId = new Map(plan.workItems.map((item) => [item.id, item]));
  let reported = false;
  const visit = (id: string): void => {
    if (reported || state.get(id) === 2) return;
    if (state.get(id) === 1) {
      const start = stack.indexOf(id);
      issues.push({ path: "/workItems", message: `dependency cycle: ${[...stack.slice(start), id].join(" -> ")}` });
      reported = true;
      return;
    }
    state.set(id, 1);
    stack.push(id);
    for (const dependency of byId.get(id)?.dependsOn ?? []) if (byId.has(dependency)) visit(dependency);
    stack.pop();
    state.set(id, 2);
  };
  for (const item of plan.workItems) visit(item.id);
}

function sourceRefIdentity(ref: DagPlanningPlanV1["source"]["refs"][number]): string {
  if (ref.kind === "project_model_object") return `${ref.kind}:${ref.collection}:${ref.objectId}`;
  if (ref.kind === "generated_spec") return `${ref.kind}:${ref.path}`;
  return `${ref.kind}:${ref.ref}`;
}

function isRepositoryRelativePath(path: string): boolean {
  if (path.startsWith("/") || path.startsWith("\\") || path.includes("\\") || path.includes("//")) return false;
  const segments = path.split("/");
  return segments.every((segment) => segment.length > 0 && segment !== "." && segment !== "..");
}

function validateValidationCommands(values: readonly { id: string; argv: string[] }[], path: string, issues: ValidationIssue[]): void {
  pushIssue(issues, path, Buffer.byteLength(JSON.stringify(values)) <= 4_000, "encoded command profile must not exceed 4000 bytes");
  for (const [index, command] of values.entries()) {
    pushIssue(issues, `${path}/${index}/argv/0`, !command.argv[0].includes("\0"), "executable must not contain NUL");
    for (const [argumentIndex, argument] of command.argv.entries()) pushIssue(issues, `${path}/${index}/argv/${argumentIndex}`, !argument.includes("\0"), "argument must not contain NUL");
  }
}

function uniqueIds(values: readonly { id: string }[], path: string, issues: ValidationIssue[]): Set<string> {
  const ids = values.map(({ id }) => id);
  uniqueStrings(ids, path, issues);
  return new Set(ids);
}

function uniqueStrings(values: readonly string[], path: string, issues: ValidationIssue[]): void {
  const seen = new Set<string>();
  for (const [index, value] of values.entries()) {
    pushIssue(issues, `${path}/${index}`, !seen.has(value), `duplicates ${JSON.stringify(value)}`);
    seen.add(value);
  }
}

function isNonempty(value: string | null): boolean {
  return typeof value === "string" && value.trim().length > 0;
}
