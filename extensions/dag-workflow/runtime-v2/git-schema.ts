import { Type, type Static } from "typebox";
import { StrictObject } from "../dag-runtime/common.ts";
import { TextV2, CountV2 } from "../planning/v2.ts";
import { CandidateV2Schema, ExecutionRequestV2Schema, NodeWorkspaceV2Schema } from "./lifecycle-schema.ts";

const identity = StrictObject({ path: TextV2, dev: Type.String({ pattern: "^[0-9]+$" }), ino: Type.String({ pattern: "^[0-9]+$" }) });
export const GitBindingV2Schema = StrictObject({ root: identity, common: identity, admin: identity,
  objectFormat: Type.Union([Type.Literal("sha1"), Type.Literal("sha256")]), refBackend: Type.Literal("files"), gitVersion: TextV2 });
export const GitOperationV2Schema = StrictObject({ operationId: TextV2, itemId: TextV2, generation: CountV2,
  reservation: TextV2, lease: StrictObject({ sessionId: TextV2, pid: Type.Integer({ minimum: 1 }), processStart: TextV2, generation: CountV2 }), candidate: CandidateV2Schema, sourceBase: CandidateV2Schema, expected: CandidateV2Schema,
  targetRef: TextV2, binding: GitBindingV2Schema, profile: Type.Union([Type.Literal("ordinary-ff-v2-1"), Type.Literal("ordinary-ff-v2-2")]),
  composition: Type.Optional(StrictObject({ version: Type.Literal("accepted-prefix-v1"), baseline: CandidateV2Schema,
    accepted: Type.Array(StrictObject({ operationId: TextV2, proposal: CandidateV2Schema })) })),
  phase: Type.Union((["intent", "composed", "validated", "landing", "landed", "accepted", "blocked", "closed"] as const).map(x => Type.Literal(x))),
  proposal: Type.Optional(CandidateV2Schema), checks: Type.Array(ExecutionRequestV2Schema),
  workspace: Type.Optional(StrictObject({ node: NodeWorkspaceV2Schema, binding: GitBindingV2Schema, closing: Type.Optional(Type.Boolean()),
    phase: Type.Union((["original", "switching", "composed", "restoring", "restored"] as const).map(x => Type.Literal(x))),
    transition: Type.Optional(StrictObject({ directory: TextV2, target: CandidateV2Schema,
      supervisor: Type.Optional(StrictObject({ pid: Type.Integer({ minimum: 1 }), processStart: TextV2, token: TextV2 })), settled: Type.Boolean() })),
  })),
  dispatches: Type.Integer({ minimum: 0, maximum: 2 }),
  landing: Type.Optional(StrictObject({ directory: TextV2, owner: StrictObject({ pid: Type.Integer({ minimum: 1 }), processStart: TextV2 }),
    supervisor: Type.Optional(StrictObject({ pid: Type.Integer({ minimum: 1 }), processStart: TextV2, token: TextV2 })), settled: Type.Boolean() })),
  observation: Type.Optional(Type.Union((["old-clean", "new-clean", "old-dirty", "new-dirty", "third", "identity-drift"] as const).map(x => Type.Literal(x)))),
  diagnostic: Type.Optional(Type.String({ maxLength: 16384 })),
});
export type GitBindingV2 = Static<typeof GitBindingV2Schema>;
export type GitOperationV2 = Static<typeof GitOperationV2Schema>;
