import type { DagPlanningPlanSummaryV1, DagPlanningPlanV1, DagPlanningWorkItemV1 } from "./types.ts";

export class DagPlanningSelectorError extends Error {
  readonly selector: string | null;
  readonly candidates: string[];

  constructor(message: string, selector: string | null, candidates: string[] = []) {
    super(message);
    this.name = "DagPlanningSelectorError";
    this.selector = selector;
    this.candidates = candidates.slice(0, 20);
  }
}

/**
 * Accepted plan selectors are an exact plan ID (the head revision) or an exact
 * `planId@revision`. Hashes deliberately do not select revisioned records.
 */
export function selectDagPlanningPlanV1(plans: readonly DagPlanningPlanV1[], selector?: string): DagPlanningPlanV1 {
  const ordered = [...plans].sort(comparePlans);
  const heads = headPlans(ordered);
  if (selector === undefined) {
    if (heads.length === 1) return heads[0];
    if (heads.length === 0) throw new DagPlanningSelectorError("No DAG planning records are available", null);
    throw new DagPlanningSelectorError(
      "Plan selector is required because multiple DAG planning records are available; use an exact plan ID for its head or planId@revision",
      null,
      heads.map(({ planId }) => planId),
    );
  }
  if (!selector) throw new DagPlanningSelectorError("Plan selector must be a non-empty exact plan ID or planId@revision", selector);

  const revisionSelector = /^(?<planId>[A-Za-z0-9][A-Za-z0-9._-]{0,127})@(?<revision>[1-9][0-9]*)$/.exec(selector);
  const matches = revisionSelector
    ? ordered.filter((plan) => plan.planId === revisionSelector.groups!.planId && plan.revision === Number(revisionSelector.groups!.revision))
    : heads.filter((plan) => plan.planId === selector);
  if (matches.length === 1) return matches[0];
  if (matches.length === 0) {
    throw new DagPlanningSelectorError(
      `No DAG planning record exactly matches ${JSON.stringify(selector)}; use an exact plan ID for its head or planId@revision`,
      selector,
      acceptedCandidates(ordered),
    );
  }
  throw new DagPlanningSelectorError(`Plan selector ${JSON.stringify(selector)} is ambiguous`, selector, matches.map(exactRevisionSelector));
}

export function selectDagPlanningWorkItemV1(plan: DagPlanningPlanV1, selector: string): { item: DagPlanningWorkItemV1; alias: string; index: number } {
  if (!selector) throw new DagPlanningSelectorError("Node selector must be a non-empty exact work-item ID or node alias", selector);
  const aliases = dagPlanningNodeAliasesV1(plan);
  const matches = plan.workItems.flatMap((item, index) => item.id === selector || aliases[index] === selector ? [{ item, alias: aliases[index], index }] : []);
  if (matches.length === 1) return matches[0];
  const candidates = plan.workItems.flatMap((item, index) => [item.id, aliases[index]]);
  if (matches.length === 0) throw new DagPlanningSelectorError(`No work item exactly matches ${JSON.stringify(selector)}`, selector, candidates);
  throw new DagPlanningSelectorError(`Node selector ${JSON.stringify(selector)} is ambiguous`, selector, matches.flatMap(({ item, alias }) => [item.id, alias]));
}

export function dagPlanningNodeAliasesV1(plan: Pick<DagPlanningPlanV1, "workItems">): string[] {
  const width = Math.max(2, String(plan.workItems.length).length);
  return plan.workItems.map((_, index) => `N${String(index + 1).padStart(width, "0")}`);
}

export function summarizeDagPlanningPlanV1(plan: DagPlanningPlanV1): DagPlanningPlanSummaryV1 {
  return { planId: plan.planId, revision: plan.revision, status: plan.status, title: plan.title, planHash: plan.planHash };
}

function headPlans(plans: readonly DagPlanningPlanV1[]): DagPlanningPlanV1[] {
  const highestRevision = new Map<string, number>();
  for (const plan of plans) highestRevision.set(plan.planId, Math.max(highestRevision.get(plan.planId) ?? 0, plan.revision));
  return plans
    .filter((plan) => plan.revision === highestRevision.get(plan.planId))
    .sort(comparePlans);
}

function acceptedCandidates(plans: readonly DagPlanningPlanV1[]): string[] {
  const heads = headPlans(plans);
  return [
    ...heads.map(({ planId }) => planId),
    ...plans.map(exactRevisionSelector),
  ];
}

function exactRevisionSelector(plan: DagPlanningPlanV1): string {
  return `${plan.planId}@${plan.revision}`;
}

function comparePlans(left: DagPlanningPlanV1, right: DagPlanningPlanV1): number {
  return left.planId.localeCompare(right.planId) || left.revision - right.revision || left.planHash.localeCompare(right.planHash);
}
