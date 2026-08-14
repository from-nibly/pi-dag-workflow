export const DAG_PLANNING_SCHEMA_VERSION = 1 as const;
export const DEFAULT_DAG_PLANNING_DIRECTORY = ".ai/dag-plans-v1/plans";

export type DagPlanningStatusV1 = "draft" | "ready" | "superseded";
export type DagPlanningRiskV1 = "low" | "medium" | "high";

export type DagPlanningModelCollectionV1 =
  | "workstreams"
  | "intents"
  | "concepts"
  | "evidence"
  | "assumptions"
  | "questions"
  | "tensions"
  | "scenarios"
  | "proposals"
  | "decisions"
  | "commitments"
  | "discoveries";

export type DagPlanningSourceRefV1 =
  | {
      kind: "project_model_object";
      collection: DagPlanningModelCollectionV1;
      objectId: string;
      semanticHash: string;
      summary?: string;
    }
  | {
      kind: "generated_spec";
      path: string;
      contentHash: string;
      summary?: string;
    }
  | {
      kind: "external";
      ref: string;
      summary?: string;
    };

export interface DagPlanningOutcomeV1 {
  id: string;
  description: string;
}

export interface DagPlanningRepositoryV1 {
  repositoryId: string;
  baselineCommit: string;
  baselineTree: string;
  targetBranch: string;
}

export interface DagPlanningWorkItemV1 {
  id: string;
  title: string;
  objective: string;
  outcomeIds: string[];
  context: string[];
  checks: string[];
  dependsOn: string[];
  risk: DagPlanningRiskV1;
  riskNotes: string[];
  constraints?: string[];
}

export interface DagPlanningMutexGroupV1 {
  id: string;
  workItemIds: string[];
  reason: string;
}

export interface DagPlanningValidationCommandV1 {
  id: string;
  argv: string[];
}

export interface DagPlanningApprovalV1 {
  status: "pending" | "approved" | "rejected";
  by: string | null;
  at: string | null;
  note: string | null;
}

export interface DagPlanningAuthorizationV1 {
  status: "not_authorized" | "authorized" | "revoked";
  by: string | null;
  at: string | null;
  scope: string[];
  maxConcurrency: number | null;
  note: string | null;
}

export interface DagPlanningDecisionStateV1 {
  status: DagPlanningStatusV1;
  approval: DagPlanningApprovalV1;
  authorization: DagPlanningAuthorizationV1;
}

export interface DagPlanningPlanV1 {
  schemaVersion: typeof DAG_PLANNING_SCHEMA_VERSION;
  kind: "dag_planning_record";
  planId: string;
  revision: number;
  status: DagPlanningStatusV1;
  title: string;
  focusId: string | null;
  createdAt: string;
  updatedAt: string;
  repository: DagPlanningRepositoryV1;
  source: {
    refs: DagPlanningSourceRefV1[];
    scopeSummary: string;
  };
  architecture: {
    outcomes: DagPlanningOutcomeV1[];
    nonGoals: string[];
    notes: string[];
    risks: string[];
  };
  workItems: DagPlanningWorkItemV1[];
  constraints: {
    maxConcurrency: number | null;
    mutexGroups: DagPlanningMutexGroupV1[];
  };
  integration: {
    strategy: "dependency_order" | "serial";
    checks: string[];
    finalChecks: string[];
    prefixCommands: DagPlanningValidationCommandV1[];
    finalCommands: DagPlanningValidationCommandV1[];
  };
  approval: DagPlanningApprovalV1;
  authorization: DagPlanningAuthorizationV1;
  /** Hash of static semantic plan content. Decision and revision metadata are excluded. */
  planHash: string;
}

export type DagPlanningPlanInputV1 = Omit<
  DagPlanningPlanV1,
  "schemaVersion" | "kind" | "revision" | "createdAt" | "updatedAt" | "planHash"
>;

export interface DagPlanningPlanSummaryV1 {
  planId: string;
  revision: number;
  status: DagPlanningStatusV1;
  title: string;
  planHash: string;
}
