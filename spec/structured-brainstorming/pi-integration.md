# Pi extension integration

> **Transitional contract:** The [mixed-initiative project-model integration](../mixed-initiative-project-model/spec.md#production-brainstorming-integration) supersedes this document's five-tool, per-snapshot, promotion, session-replacement, and renderer assumptions. Retain this file only as migration evidence until the production cutover replaces it.

## Production module boundary

Structured brainstorming is implemented as a bounded TypeScript module under `extensions/dag-workflow/brainstorm/` inside the existing DAG extension. The module owns typed state access, five-tool schemas/adapters, command lifecycle, mode guidance, compact tool rendering, and optional presentation adapters.

The existing legacy `/dag brainstorm` discovery prompt is replaced directly. The runnable object-tools prototype and [Pi-adapter prototype](../prototypes/brainstorm-pi-adapter/README.md) under `spec/prototypes/` remain independent behavioral evidence and are not imported as production runtime code. No second brainstorm extension or compatibility command is maintained.

## `/dag brainstorm` command

The command owns snapshot discovery and selection before an agent turn begins.

In TUI and RPC modes:

1. list unfinished `.ai/brainstorm/*.json` snapshots;
2. when any exist, ask whether to Resume or start New;
3. if Resume is selected and several snapshots exist, ask which one;
4. request only missing identity or seed information for New;
5. activate brainstorm mode and send a compact kickoff naming the selected state, seed, orientation, and immediate action.

Print and JSON modes cannot display command dialogs. When selection is ambiguous, they require explicit continue/new arguments and report concise available choices rather than delegating selection to the agent. The command never silently resumes the latest snapshot.

## Runtime mode and tool activation

The five tools are registered once and inactive by default:

- `dag_brainstorm_context`
- `dag_brainstorm_update`
- `dag_brainstorm_review`
- `dag_brainstorm_resolve_review`
- `dag_brainstorm_promote`

Entering `/dag brainstorm` activates exactly these additions without replacing tools activated by Pi or other extensions. Brainstorm mode remains active across the multi-turn exploration. Starting another `/dag` workflow or explicitly stopping brainstorming suspends its mode guidance and removes only these five tools; the snapshot remains resumable.

Reload or session replacement does not guess which snapshot to resume. The user re-enters through `/dag brainstorm`.

Mutating adapters participate in Pi's per-file mutation queue for the selected snapshot so parallel tool calls cannot overwrite one another.

## Agent instruction delivery

While brainstorm mode is active, `before_agent_start` appends the operational behavior contract covering:

- research before asking;
- tangent discovery and user-controlled focus;
- question quality and real options;
- Initial/Current understanding;
- correction and contradiction handling;
- renderer/submission interpretation boundaries;
- promotion and user-controlled planning.

The command kickoff contains only selected-state orientation, seed, user arguments, and the immediate next action. Tool descriptions stay concise and do not duplicate the mode contract through large `promptSnippet` or `promptGuidelines` blocks.

**Rationale:** mode guidance survives long exploration and compaction while unrelated Pi sessions avoid its instruction and tool-schema cost.

## Pi tool adapters

Each adapter exposes a strict TypeBox schema and delegates domain behavior to the production brainstorm module. Model-facing `content` is a compact receipt or narrow projection. Structured `details` carries the minimum data required for custom TUI rows and expansion.

Tool renderers show operation, revision, changed IDs/counts, and short next-action guidance. They do not echo complete state, authored context, HTML documents, or renderer submissions.

The `.ai/brainstorm/` snapshot remains workflow authority. Pi session entries and tool-result details are presentation/session records and never become a second brainstorm datastore.

## Review presentation adapter

`dag_brainstorm_review` always creates the same ephemeral review packet and may receive a presentation choice that affects execution but is not persisted as datastore semantics.

### Chat presentation

The tool returns a compact review projection. The agent presents the complete turn as ordinary chat Markdown, beginning with Initial/Current understanding, and interprets the user's later chat response.

### Lavish presentation

The adapter renders the complete brainstorm-turn shell, including understanding, navigation, question cards, standard controls, and agent-authored freeform context. It opens the review surface and waits for selections, annotations, custom-context interactions, or chat submission. The unprocessed submission returns only through the tool result to the agent.

The review remains active until the agent interprets that submission and calls `dag_brainstorm_resolve_review` with semantic outcomes. Renderer submission never mutates brainstorm state directly.

If Lavish presentation is requested but unavailable or fails, the tool reports the failure and leaves the review active. It does not silently degrade a rich-only context section into Markdown.

## GrillMe retirement

`/dag grillme` is retired in favor of structured brainstorming. Production integration removes:

- the GrillMe command branch;
- `dag_grillme_*` tools;
- the GrillMe editor and active-session wiring;
- GrillMe command prompts;
- GrillMe-specific README guidance;
- production registration and state creation.

Existing ephemeral `.ai/grillme/` state has no migration requirement and may be ignored or removed during cleanup. No compatibility alias is provided.

**Rationale:** structured brainstorming now owns research-backed uncertainty, durable questions, contextual multi-point review, flexible presentation, semantic outcome interpretation, and current-understanding synthesis. Retaining GrillMe would create two competing question authorities without a distinct accepted use case.
