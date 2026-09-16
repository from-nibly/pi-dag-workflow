import { Type, type Static } from "typebox";
import { StrictObject, GitOidSchema } from "../dag-runtime/common.ts";
import { CountV2, IdV2, TextV2, PlanSelectorV2Schema, LifecycleCheckV2Schema, StageV2Schema } from "../planning/v2.ts";

const nat = Type.Integer({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER });
const output = Type.String({ maxLength: 16384 });
export const CandidateV2Schema = StrictObject({ commit: GitOidSchema, tree: GitOidSchema });
export const FindingV2Schema = StrictObject({ id: IdV2,
  kind: Type.Union((["product_defect", "test_evidence_gap", "architecture_issue", "oracle_contract_issue", "infrastructure_failure", "capability_absent", "external_precondition_failure", "equivalent_nonactionable"] as const).map(x => Type.Literal(x))),
  severity: Type.Union([Type.Literal("advisory"), Type.Literal("blocking")]),
  materiality: Type.Union([Type.Literal("local"), Type.Literal("plan_affecting")]),
  subject: TextV2, fingerprint: TextV2, detail: TextV2,
});
export const ExecutionRequestV2Schema = StrictObject({ id: IdV2, plan: PlanSelectorV2Schema, runId: IdV2, itemId: IdV2,
  generation: CountV2, attempt: TextV2, round: CountV2, stage: StageV2Schema, candidate: CandidateV2Schema,
  implementationWorkerId: TextV2, check: LifecycleCheckV2Schema,
  authority: StrictObject({ effect: Type.Literal("repository_local"), expiresAt: Type.Integer({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }) }),
});
export const ExecutionResultV2Schema = StrictObject({ request: ExecutionRequestV2Schema,
  startedAt: nat, endedAt: nat, durationMs: nat,
  disposition: Type.Union([Type.Literal("PASS"), Type.Literal("FAIL"), Type.Literal("BLOCKED")]),
  exitCode: Type.Union([Type.Integer(), Type.Null()]), signal: Type.Union([TextV2, Type.Null()]),
  stdout: output, stderr: output, truncated: Type.Boolean(), diagnostic: output,
  environment: StrictObject({ profile: TextV2, platform: TextV2, runtime: TextV2 }),
  executor: StrictObject({ kind: Type.Union([Type.Literal("command"), Type.Literal("producer")]), identity: TextV2,
    contextId: TextV2, lineage: Type.Array(TextV2, { maxItems: 32 }), invoked: Type.Boolean() }),
  workspace: StrictObject({ candidate: CandidateV2Schema, cleanBefore: Type.Boolean(), cleanAfter: Type.Boolean(), isolated: Type.Boolean() }),
  findings: Type.Array(FindingV2Schema, { maxItems: 32 }),
});
export const ExecutionV2Schema = StrictObject({ request: ExecutionRequestV2Schema,
  status: Type.Union([Type.Literal("intent"), Type.Literal("observed"), Type.Literal("quarantined")]),
  result: Type.Optional(ExecutionResultV2Schema), quarantineReason: Type.Optional(TextV2),
  contextRejection: Type.Optional(StrictObject({ reason: Type.Literal("EVALUATOR_CONTEXT_REUSED"), observed: ExecutionResultV2Schema })),
});
export const RetryDimensionV2Schema = Type.Union((["product", "test", "review", "hardening", "infrastructure", "replacement", "integration"] as const).map(x => Type.Literal(x)));
export const RetryV2Schema = StrictObject({ dimension: RetryDimensionV2Schema, stage: nat, procedure: TextV2, fingerprint: TextV2,
  count: nat, trees: Type.Array(GitOidSchema), failures: Type.Array(TextV2) });
export const LifecycleV2Schema = StrictObject({ candidate: CandidateV2Schema, candidateReady: Type.Boolean(), candidates: Type.Array(CandidateV2Schema), round: CountV2,
  stage: Type.Integer({ minimum: 1, maximum: 8 }), passed: Type.Array(Type.Integer({ minimum: 0, maximum: 8 }), { uniqueItems: true }),
  frame: StrictObject({ plan: PlanSelectorV2Schema, baseline: CandidateV2Schema, oracle: TextV2, risk: TextV2, checks: Type.Array(IdV2), at: nat }),
  executions: Type.Array(ExecutionV2Schema), findings: Type.Array(StrictObject({ finding: FindingV2Schema, disposition: Type.Optional(TextV2) })),
  ready: Type.Boolean(), stop: Type.Optional(TextV2),
});
export const CommandJobV2Schema = StrictObject({ request: ExecutionRequestV2Schema, owner: StrictObject({ pid: Type.Integer({ minimum: 1, maximum: 2147483647 }), processStart: TextV2 }),
  status: Type.Union([Type.Literal("running"), Type.Literal("settled"), Type.Literal("ambiguous")]),
  workspace: Type.Optional(TextV2),
  result: Type.Optional(ExecutionResultV2Schema),
});
export type CandidateV2 = Static<typeof CandidateV2Schema>;
export type FindingV2 = Static<typeof FindingV2Schema>;
export type ExecutionRequestV2 = Static<typeof ExecutionRequestV2Schema>;
export type ExecutionResultV2 = Static<typeof ExecutionResultV2Schema>;
export type LifecycleV2 = Static<typeof LifecycleV2Schema>;
export type RetryV2 = Static<typeof RetryV2Schema>;
export type RetryDimensionV2 = Static<typeof RetryDimensionV2Schema>;
export type CommandJobV2 = Static<typeof CommandJobV2Schema>;
