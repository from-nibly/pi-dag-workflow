export const AI_DIR = ".ai";
export const DEFAULT_DAG_PATH = ".ai/dag.json";
export const DEFAULT_PROJECT_PATH = ".ai/project.md";
export const USER_CONFIG_PATH = "~/.pi/agent/extensions/dag-workflow/config.json";

export type ThinkingLevel = "off" | "minimal" | "low" | "medium" | "high" | "xhigh";
export type StepKind = "agent" | "merge";
export type StepStatus = "pending" | "running" | "passed" | "failed" | "skipped";
export type NodeStatus = "pending" | "running" | "needs_decision" | "merge_ready" | "merged" | "failed";
export type RunStatus = "running" | "completed" | "failed" | "cancelled";

export interface DagStep {
  id: string;
  kind: StepKind;
  agent?: string;
  model?: string;
  thinking?: ThinkingLevel;
  prompt?: string;
  input?: string;
  output?: string;
  requires?: string[];
  onFail?: string;
  onConflict?: string;
  [key: string]: unknown;
}

export type DagFlowStep = Partial<DagStep> & { id: string };

export interface DagNode {
  id: string;
  title: string;
  flow?: string;
  chunkFile: string;
  dependsOn: string[];
  ownedFiles: string[];
  forbiddenFiles: string[];
  setupInstructions?: string;
  implementationInstructions?: string;
  validationInstructions?: string;
  [key: string]: unknown;
}

export interface DagEdge {
  from: string;
  to: string;
  type?: "hard" | "soft" | "file-conflict" | "merge-order" | string;
  reason?: string;
}

export interface DagFile {
  schemaVersion: 1;
  run: {
    name: string;
    brief?: string;
    plan: string;
    chunksDir?: string;
    maxConcurrency: number;
  };
  defaults: {
    flow: string;
    stashDirtyParent?: boolean;
    mergeStrategy?: "merge-no-ff" | string;
    [key: string]: unknown;
  };
  steps: DagStep[];
  merge: DagStep;
  flows: Record<string, DagFlowStep[]>;
  nodes: DagNode[];
  edges: DagEdge[];
}

export interface DagWorkflowConfig {
  defaults?: Partial<DagFile["defaults"]>;
  steps?: Array<Partial<DagStep> & { id: string }>;
  merge?: Partial<DagStep> & { id?: string };
  flows?: Record<string, DagFlowStep[]>;
  nodeFlowOverrides?: Array<{ match: string; flow: string }>;
  [key: string]: unknown;
}

export interface ValidationResult {
  valid: boolean;
  errors: string[];
  warnings: string[];
  dag?: DagFile;
}

export interface StepAttemptState {
  stepId: string;
  flowIndex: number;
  attempt: number;
  status: StepStatus;
  startedAt?: string;
  endedAt?: string;
  artifactPath?: string;
  finalText?: string;
  verdict?: "PASS" | "FAIL";
  model?: string;
  workerId?: string;
  sessionFile?: string;
  exitCode?: number;
  decision?: string;
}

export interface NodeRunState {
  id: string;
  status: NodeStatus;
  flow: string;
  currentFlowIndex: number;
  attempts: StepAttemptState[];
  branch?: string;
  worktree?: string;
  baseCommit?: string;
  currentCommit?: string;
  failureReason?: string;
  updatedAt: string;
  mergedAt?: string;
}

export interface RunManifest {
  runId: string;
  dagPath: string;
  dagName: string;
  cwd: string;
  parentBranch: string;
  baseCommit: string;
  createdAt: string;
  updatedAt: string;
  status: RunStatus;
}

export interface RunState {
  manifest: RunManifest;
  nodes: Record<string, NodeRunState>;
}

export interface WorkerResultInput {
  nodeId: string;
  flowIndex?: number;
  stepId?: string;
  attempt?: number;
  finalText?: string;
  outputPath?: string;
  exitCode?: number;
  sessionFile?: string;
  model?: string;
  errorMessage?: string;
}
