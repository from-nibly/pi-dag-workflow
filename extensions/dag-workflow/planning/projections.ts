import { assertDagPlanningPlanV1 } from "./artifact.ts";
import { dagPlanningNodeAliasesV1, selectDagPlanningWorkItemV1 } from "./selectors.ts";
import type {
  DagPlanningMutexGroupV1,
  DagPlanningOutcomeV1,
  DagPlanningPlanV1,
  DagPlanningWorkItemV1,
} from "./types.ts";

export interface DagPlanningGraphNodeV1 {
  alias: string;
  workItemId: string;
  title: string;
  risk: DagPlanningWorkItemV1["risk"];
}

export interface DagPlanningGraphEdgeV1 {
  from: string;
  to: string;
  fromAlias: string;
  toAlias: string;
}

export interface DagPlanningGraphProjectionV1 {
  schemaVersion: 1;
  kind: "dag_planning_static_graph";
  planId: string;
  revision: number;
  recordSelector: string;
  planHash: string;
  nodes: DagPlanningGraphNodeV1[];
  edges: DagPlanningGraphEdgeV1[];
  mutexGroups: DagPlanningMutexGroupV1[];
}

export interface DagPlanningNodeProjectionV1 {
  schemaVersion: 1;
  kind: "dag_planning_node";
  planId: string;
  revision: number;
  recordSelector: string;
  planHash: string;
  alias: string;
  workItem: DagPlanningWorkItemV1;
  outcomes: DagPlanningOutcomeV1[];
  dependencies: Array<{ alias: string; workItemId: string; title: string }>;
  dependents: Array<{ alias: string; workItemId: string; title: string }>;
  repository: DagPlanningPlanV1["repository"];
  source: DagPlanningPlanV1["source"];
  integration: DagPlanningPlanV1["integration"];
  approval: DagPlanningPlanV1["approval"];
  authorization: DagPlanningPlanV1["authorization"];
}

export interface DagPlanningLineageRevisionV1 {
  selector: string;
  revision: number;
  status: DagPlanningPlanV1["status"];
  planHash: string;
  createdAt: string;
  updatedAt: string;
  isHead: boolean;
}

export interface DagPlanningLineageProjectionV1 {
  schemaVersion: 1;
  kind: "dag_planning_lineage";
  planId: string;
  headSelector: string;
  headRevision: number;
  revisions: DagPlanningLineageRevisionV1[];
}

export function projectDagPlanningGraphV1(plan: DagPlanningPlanV1): DagPlanningGraphProjectionV1 {
  assertDagPlanningPlanV1(plan);
  const aliases = dagPlanningNodeAliasesV1(plan);
  const indexById = new Map(plan.workItems.map((item, index) => [item.id, index]));
  return {
    schemaVersion: 1,
    kind: "dag_planning_static_graph",
    planId: plan.planId,
    revision: plan.revision,
    recordSelector: exactRecordSelector(plan),
    planHash: plan.planHash,
    nodes: plan.workItems.map((item, index) => ({ alias: aliases[index], workItemId: item.id, title: item.title, risk: item.risk })),
    edges: plan.workItems.flatMap((item, targetIndex) => item.dependsOn.map((dependency) => {
      const sourceIndex = indexById.get(dependency)!;
      return { from: dependency, to: item.id, fromAlias: aliases[sourceIndex], toAlias: aliases[targetIndex] };
    })),
    mutexGroups: structuredClone(plan.constraints.mutexGroups),
  };
}

export function renderDagPlanningGraphV1(plan: DagPlanningPlanV1): string {
  const graph = projectDagPlanningGraphV1(plan);
  const incoming = new Set(graph.edges.map(({ to }) => to));
  const lines = [
    `Plan ${graph.planId}@${graph.revision}`,
    `Hash ${graph.planHash}`,
    "",
    ...graph.nodes.map((node) => `${node.alias} [${node.risk}] ${oneLine(node.title)} (${node.workItemId})`),
    "",
    "Dependencies:",
  ];
  const roots = graph.nodes.filter(({ workItemId }) => !incoming.has(workItemId));
  if (roots.length > 0) lines.push(...roots.map(({ alias }) => `(root) -> ${alias}`));
  lines.push(...graph.edges.map(({ fromAlias, toAlias }) => `${fromAlias} -> ${toAlias}`));
  if (graph.mutexGroups.length > 0) {
    const aliasById = new Map(graph.nodes.map(({ workItemId, alias }) => [workItemId, alias]));
    lines.push("", "Concurrency constraints:");
    lines.push(...graph.mutexGroups.map((group) => `${group.id}: ${group.workItemIds.map((id) => aliasById.get(id)).join(" ~ ")} — ${oneLine(group.reason)}`));
  }
  return `${lines.join("\n")}\n`;
}

export function projectDagPlanningNodeV1(plan: DagPlanningPlanV1, selector: string): DagPlanningNodeProjectionV1 {
  assertDagPlanningPlanV1(plan);
  const selected = selectDagPlanningWorkItemV1(plan, selector);
  const aliases = dagPlanningNodeAliasesV1(plan);
  const indexById = new Map(plan.workItems.map((item, index) => [item.id, index]));
  const brief = (item: DagPlanningWorkItemV1) => {
    const index = indexById.get(item.id)!;
    return { alias: aliases[index], workItemId: item.id, title: item.title };
  };
  return {
    schemaVersion: 1,
    kind: "dag_planning_node",
    planId: plan.planId,
    revision: plan.revision,
    recordSelector: exactRecordSelector(plan),
    planHash: plan.planHash,
    alias: selected.alias,
    workItem: structuredClone(selected.item),
    outcomes: structuredClone(plan.architecture.outcomes.filter(({ id }) => selected.item.outcomeIds.includes(id))),
    dependencies: selected.item.dependsOn.map((id) => brief(plan.workItems[indexById.get(id)!])),
    dependents: plan.workItems.filter(({ dependsOn }) => dependsOn.includes(selected.item.id)).map(brief),
    repository: structuredClone(plan.repository),
    source: structuredClone(plan.source),
    integration: structuredClone(plan.integration),
    approval: structuredClone(plan.approval),
    authorization: structuredClone(plan.authorization),
  };
}

export function projectDagPlanningLineageV1(plans: readonly DagPlanningPlanV1[]): DagPlanningLineageProjectionV1 {
  if (plans.length === 0) throw new Error("DAG planning lineage requires at least one revision");
  for (const plan of plans) assertDagPlanningPlanV1(plan);
  const ordered = [...plans].sort((left, right) => left.revision - right.revision || left.planHash.localeCompare(right.planHash));
  const planId = ordered[0].planId;
  if (ordered.some((plan) => plan.planId !== planId)) throw new Error("DAG planning lineage cannot combine different plan IDs");
  for (const [index, plan] of ordered.entries()) {
    if (plan.revision !== index + 1) throw new Error(`DAG planning lineage must contain each exact revision from 1; found ${plan.revision} at position ${index + 1}`);
  }
  const head = ordered.at(-1)!;
  return {
    schemaVersion: 1,
    kind: "dag_planning_lineage",
    planId,
    headSelector: planId,
    headRevision: head.revision,
    revisions: ordered.map((plan) => ({
      selector: exactRecordSelector(plan),
      revision: plan.revision,
      status: plan.status,
      planHash: plan.planHash,
      createdAt: plan.createdAt,
      updatedAt: plan.updatedAt,
      isHead: plan === head,
    })),
  };
}

export function renderDagPlanningMarkdownV1(plan: DagPlanningPlanV1): string {
  assertDagPlanningPlanV1(plan);
  const aliases = dagPlanningNodeAliasesV1(plan);
  const aliasById = new Map(plan.workItems.map((item, index) => [item.id, aliases[index]]));
  const lines: string[] = [
    `# ${inline(plan.title)}`,
    "",
    `- Plan: \`${inline(plan.planId)}\` revision ${plan.revision} (exact record \`${exactRecordSelector(plan)}\`)`,
    `- Focus: ${plan.focusId === null ? "none" : `\`${inline(plan.focusId)}\``}`,
    `- Status: **${plan.status}**`,
    `- Static plan hash: \`${plan.planHash}\``,
    `- Repository: \`${inline(plan.repository.repositoryId)}\` at commit \`${plan.repository.baselineCommit}\`, tree \`${plan.repository.baselineTree}\``,
    `- Target branch: \`${inline(plan.repository.targetBranch)}\``,
    "",
    "## Scope",
    "",
    plan.source.scopeSummary,
    "",
    "### Governing sources",
    "",
    ...(plan.source.refs.length ? plan.source.refs.map(renderSourceRef) : ["_No source references._"]),
    "",
    "## Outcomes",
    "",
    ...plan.architecture.outcomes.map((outcome) => `- **${inline(outcome.id)}:** ${inline(outcome.description)}`),
    "",
    "## Non-goals",
    "",
    ...bullets(plan.architecture.nonGoals),
    "",
    "## Architecture",
    "",
    "### Notes",
    "",
    ...bullets(plan.architecture.notes),
    "",
    "### Risks",
    "",
    ...bullets(plan.architecture.risks),
    "",
    "## Work items",
  ];
  for (const [index, item] of plan.workItems.entries()) {
    lines.push(
      "",
      `### ${aliases[index]} · ${inline(item.title)}`,
      "",
      `- ID: \`${inline(item.id)}\``,
      `- Risk: **${item.risk}**`,
      `- Outcomes: ${item.outcomeIds.map((id) => `\`${inline(id)}\``).join(", ")}`,
      `- Dependencies: ${item.dependsOn.length ? item.dependsOn.map((id) => `\`${aliasById.get(id)}\` (\`${inline(id)}\`)`).join(", ") : "none"}`,
      "",
      item.objective,
      "",
      "**Context**",
      "",
      ...bullets(item.context),
      "",
      "**Checks**",
      "",
      ...bullets(item.checks),
      "",
      "**Risk notes**",
      "",
      ...bullets(item.riskNotes),
    );
    if (item.constraints) lines.push("", "**Constraints**", "", ...bullets(item.constraints));
  }
  lines.push(
    "",
    "## Integration",
    "",
    `- Strategy: \`${plan.integration.strategy}\``,
    `- Maximum concurrency: ${plan.constraints.maxConcurrency ?? "not constrained"}`,
    "",
    "### Integration checks",
    "",
    ...bullets(plan.integration.checks),
    "",
    "### Final checks",
    "",
    ...bullets(plan.integration.finalChecks),
    "",
    "### Executable validation commands",
    "",
    ...plan.integration.prefixCommands.map((command) => `- Prefix \`${inline(command.id)}\`: \`${inline(JSON.stringify(command.argv))}\``),
    ...plan.integration.finalCommands.map((command) => `- Final \`${inline(command.id)}\`: \`${inline(JSON.stringify(command.argv))}\``),
    "",
    "## Approval and authorization",
    "",
    `- Approval: **${plan.approval.status}**${decisionSuffix(plan.approval)}`,
    `- Authorization: **${plan.authorization.status}**${decisionSuffix(plan.authorization)}`,
    `- Authorized max concurrency: ${plan.authorization.maxConcurrency ?? "none"}`,
    `- Authorized scope: ${plan.authorization.scope.length ? plan.authorization.scope.map((entry) => inline(entry)).join("; ") : "none"}`,
  );
  return `${lines.join("\n")}\n`;
}

function exactRecordSelector(plan: Pick<DagPlanningPlanV1, "planId" | "revision">): string {
  return `${plan.planId}@${plan.revision}`;
}

function renderSourceRef(ref: DagPlanningPlanV1["source"]["refs"][number]): string {
  const detail = ref.kind === "project_model_object"
    ? `project model \`${inline(ref.collection)}:${inline(ref.objectId)}\` at \`${ref.semanticHash}\``
    : ref.kind === "generated_spec"
      ? `generated spec \`${inline(ref.path)}\` at \`${ref.contentHash}\``
      : `external \`${inline(ref.ref)}\``;
  return `- ${detail}${ref.summary ? ` — ${inline(ref.summary)}` : ""}`;
}

function bullets(values: readonly string[]): string[] {
  return values.length ? values.map((value) => `- ${inline(value)}`) : ["_None._"];
}

function inline(value: string): string {
  return value.replace(/\r?\n/g, " ").replace(/\|/g, "\\|");
}

function oneLine(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function decisionSuffix(decision: { by: string | null; at: string | null; note: string | null }): string {
  const actor = decision.by ? ` by ${inline(decision.by)}` : "";
  const at = decision.at ? ` at ${decision.at}` : "";
  const note = decision.note ? ` — ${inline(decision.note)}` : "";
  return `${actor}${at}${note}`;
}
