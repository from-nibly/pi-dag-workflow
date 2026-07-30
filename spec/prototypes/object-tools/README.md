# Brainstorm Object Tools Prototype

This is a long-lived brainstorming prototype, not production extension code. It tests the accepted renderer-neutral tool flow before Pi registration and optional rendering are added.

## Behavioral scope

The prototype demonstrates:

- one mutable, worktree-local JSON snapshot;
- typed objects connected by explicit references;
- tool-generated IDs, timestamps, revisions, defaults, and derived links;
- narrow context reads with compact AXI-like deltas and next-action reminders;
- one mutable Initial/Current understanding proof projected into orientation and reviews;
- semantic updates outside review, including materially changed understanding linked to evidence/decisions;
- renderer-neutral review packets with contextual questions, stored proposals, recommendations, and automatic `Other` projection;
- direct resolution of agent-interpreted outcomes and their replacement understanding proof without persisting verbatim responses or unprocessed renderer submissions;
- separate prepare → agent edits specs → record promotion behavior;
- proposed/tracked/active/deferred/out-of-scope/closed tangent state and derived badges.

Related specifications:

- [Structured brainstorming](../../structured-brainstorming/spec.md)
- [Object model prototype contract](../../structured-brainstorming/object-model.md)

It does **not** define Pi `registerTool` schemas/renderers, Lavish interaction or transport, production persistence, planning/chunking, archive implementation, or final visual design. Its definitions end at `BrainstormPrototype.execute()` and the file-backed scenario. Fixture names and compact result wording are examples rather than final API syntax.

## Run verification

```bash
node spec/prototypes/object-tools/operant-scenario.mjs
```

Expected observation: the command exits successfully and prints `Object tools prototype OK...`; assertion failures exit nonzero. The scenario verifies:

- user-command-style start/resume outside the five agent tools;
- mechanical defaults and explicit reference resolution;
- required context for agent-proposed tangents;
- narrow orientation/entity/promotion-ready reads and rejection of broad state views;
- one review packet with an Initial understanding proof, stored options, a reasoned recommendation, and automatic `Other` projection;
- optional-Lavish and next-tool reminders without renderer state;
- semantic resolution that replaces the proof with Current understanding;
- sparse outcome resolution where an ignored substantive question remains open;
- automatic Proposed → Tracked transition after review resolution;
- absence of `feedback` and legacy `turns` collections;
- promotion prepare that does not write files;
- agent-authored spec editing followed by promotion recording;
- changed promoted decisions becoming promotion-ready again until re-recorded;
- reload from the persisted schema-v2 snapshot;
- removal of the legacy raw-feedback tool.

## Tool runner

```bash
node spec/prototypes/object-tools/cli.mjs --help
```

Each invocation accepts a tool name and a JSON params object. Params can be inline, read from `@file.json`, or piped through stdin. `root` defaults to the current directory.

The CLI-only `start` harness simulates the user-facing `/dag brainstorm` new/resume boundary:

```bash
node spec/prototypes/object-tools/cli.mjs \
  start \
  '{"root":"/tmp/example","id":"demo","title":"Demo brainstorm"}'
```

## Agent tools

### `dag_brainstorm_context`

Narrow read views only:

- `orientation` (default)
- `frontier`
- `review`
- `entities` with explicit IDs
- `promotion_ready`

There is intentionally no full-state or whole-document view.

### `dag_brainstorm_update`

Atomically adds, patches, removes, or signals active semantic objects. Additions use collection-shaped arrays; tools allocate canonical IDs and apply operation-specific defaults. Explicit aliases/IDs establish all semantic references. A material research/update reframing may replace Current understanding with decision/evidence references.

### `dag_brainstorm_review`

Creates or replaces one renderer-neutral review packet. The first packet supplies Initial understanding; later packets project the stored proof. Points reference stored questions/proposals or create inline proposal options once. The result suggests ordinary Markdown or optional Lavish presentation, then review resolution.

### `dag_brainstorm_resolve_review`

Applies agent-interpreted point outcomes and a replacement Current understanding proof. It does not persist verbatim chat responses or unprocessed renderer selections, annotations, comments, or submission metadata. It can accept stored proposals, apply explicit modified decisions, preserve unresolved substantive points, add follow-ups, track untouched proposed tangents, and retire the packet.

### `dag_brainstorm_promote`

- `prepare` returns selected decision context and target hints without writing specs.
- `record` verifies agent-edited spec paths and records decision-to-spec targets.

## Storage

For brainstorm id `demo`:

```text
.ai/brainstorm/demo.json
```

The schema-v2 snapshot is ephemeral working memory for one agent in one worktree and is never canonical. Loading a schema-v1 prototype snapshot performs a minimal in-memory upgrade: `turns` become `reviews`, raw `feedback` is discarded, and old integration metadata becomes resolution metadata. The next mutation persists schema v2.

After the completed DAG workflow is implemented, reviewed, and accepted by the human, archive may synthesize remaining `.ai` context into tracked history and clean ephemeral state. Archive performs no verification gate.

## Intentional limitations

- No Pi extension adapter or TypeBox schemas yet.
- No Markdown or Lavish renderer; review packets are renderer-neutral.
- No event sourcing, durable provenance, revision matching, multi-process locking, or cross-worktree coordination.
- No durable migration framework beyond the minimal disposable v1→v2 upgrade.
- No semantic inference from prose: tools do not judge tangent value, question quality, recommendations, user intent, contracts, or equivalence.
- AXI-like next reminders are structural suggestions, not workflow gates.
- Promotion does not merge or generate canonical Markdown; the agent edits specs with normal coding tools.
