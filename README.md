# pi-dag-workflow

Standalone Pi package for DAG-oriented project discovery, planning, chunking, and ordered subagent execution.

## Default behavior

The packaged prompts guide project discovery, decision interrogation, planning, chunking, worker discipline, and validation while preserving the default `setup -> execute -> validate` node flow. Project-specific setup, implementation, and validation details still belong in `.ai/project.md`, `.ai/plan.md`, chunks, and node instructions.

## Install

```nu
pi install git:git@github.com:from-nibly/pi-dag-workflow@v0.1.0
```

DAG execution uses the `subagent` tool from `pi-subagents`. Install or enable `pi-subagents` once, separately:

```nu
pi install npm:pi-subagents@0.25.0
```

`pi-dag-workflow` intentionally does not auto-load its dependency copy of the `pi-subagents` extension, because many users already have `pi-subagents` installed globally and duplicate loading causes tool registration conflicts.

## Commands

All user commands are under `/dag`:

```text
/dag brainstorm   # create/update .ai/project.md conversationally
/dag grillme      # interactive GrillMe TUI for many questions
/dag plan         # write .ai/plan.md
/dag chunk        # write .ai/chunks and .ai/dag.json
/dag validate     # validate .ai/dag.json
/dag run          # start conductor prompt
/dag status       # show latest run status
/dag workers      # summarize worker records
/dag review       # reviewer prompt
/dag retro        # retrospective prompt
/dag archive      # write .ai/history entry, then ask about cleanup
```

## Usage flow

You can use any combination of `/dag brainstorm`, `/dag grillme`, and `/dag plan` as long as you end up with `.ai/plan.md`. Then `/dag chunk` creates `.ai/chunks/*` and `.ai/dag.json`, and `/dag run` starts the top-level conductor agent.

```mermaid
flowchart TD
  Start([Start in a project]) --> Discover{Need more project understanding?}

  Discover -->|optional| Brainstorm["/dag brainstorm\nConversational project discovery\nupdates .ai/project.md"]
  Discover -->|optional| GrillMe["/dag grillme\nQuestion TUI + chat mode\nupdates .ai/grillme/* and .ai/project.md"]
  Discover -->|optional| Existing["Use an existing .ai/plan.md"]

  Brainstorm --> Plan["/dag plan\nwrite/update .ai/plan.md"]
  GrillMe --> Plan
  Existing --> PlanReady[".ai/plan.md"]
  Plan --> PlanReady

  PlanReady --> Chunk["/dag chunk\nwrite .ai/chunks/chunk-N.md\nwrite .ai/dag.json"]
  Chunk --> Validate["/dag validate\noptional schema/config check"]
  Validate --> Run["/dag run\nstart top-level conductor agent"]
  Chunk --> Run

  Run --> NextAction["conductor calls dag_next_action"]
  NextAction --> StartNode["dag_start_node\nreturns subagent launch params"]
  StartNode --> Worker["worker/reviewer subagents\nsetup → execute → validate"]
  Worker --> Record["dag_record_worker_result"]
  Record --> Decide{Next action?}
  Decide -->|more ready nodes| StartNode
  Decide -->|merge ready| Merge["dag_merge_node"]
  Merge --> Decide
  Decide -->|blocked/failure/conflict| UserInput["You steer the top-level agent\nchat instructions, answer questions, choose retry/skip/fix"]
  UserInput --> NextAction
  Decide -->|all merged| Finalize["dag_finalize"]
  Finalize --> Done([Done])
  Done --> Archive["/dag archive\nwrite .ai/history entry\nthen ask about cleanup"]

  Status["/dag status / workers / inspect / tail\nobserve progress anytime"] -.-> Run
  Status -.-> UserInput
```

During `/dag run`, you steer the **top-level conductor agent**, not each worker directly. You can provide additional instructions in chat, ask it to inspect status, choose recovery actions, or stop and revise chunks/config before continuing.

After a run or planning cycle, `/dag archive` writes a durable history file such as `.ai/history/YYYY-MM-DD-HH-MM-<type>-<slug>.md` first, then asks whether you want to clean up old DAG artifacts. Cleanup is never automatic; files are deleted or moved only after explicit confirmation after the history file has been created.

## Config

- User-global config: `~/.pi/agent/extensions/dag-workflow/config.json`
- Project config: `.ai/dag.config.json`

Top-level `steps` is an array of reusable step definition objects merged by `id`. Top-level `flows` is a map of flow names to ordered arrays of flow step objects. Flow step objects require `id` and may override any step field, including `agent`, `model`, and `thinking`.

The packaged default flow is `setup -> execute -> validate` using `builtin:worker` and `builtin:reviewer`.

`merge` is top-level, step-shaped, has no ordering fields, and is appended implicitly after every node flow.

## DAG shape

```json
{
  "schemaVersion": 1,
  "run": {
    "name": "example",
    "plan": ".ai/plan.md",
    "maxConcurrency": 2
  },
  "defaults": { "flow": "default" },
  "steps": [
    { "id": "setup", "kind": "agent", "agent": "builtin:worker", "prompt": "builtin:setup" },
    { "id": "execute", "kind": "agent", "agent": "builtin:worker", "prompt": "builtin:executor" },
    { "id": "validate", "kind": "agent", "agent": "builtin:reviewer", "prompt": "builtin:validator" }
  ],
  "merge": { "id": "merge", "kind": "merge", "onConflict": "resolve" },
  "flows": {
    "default": [{ "id": "setup" }, { "id": "execute" }, { "id": "validate" }]
  },
  "nodes": [
    {
      "id": "chunk-1",
      "title": "Example chunk",
      "chunkFile": ".ai/chunks/chunk-1.md",
      "dependsOn": [],
      "ownedFiles": ["src/example.ts"],
      "forbiddenFiles": []
    }
  ],
  "edges": []
}
```

Project/chunk-specific setup, implementation, and validation details belong on node instructions (`setupInstructions`, `implementationInstructions`, `validationInstructions`), not in builtin prompt markdown.

## GrillMe

`/dag grillme` installs a vim-like Pi TUI editor:

- `Esc`: nav mode
- nav: `h` previous, `l` next, `j/k` option, `Enter` answer selected option, `a` freeform answer, `c` chat, `x` complete
- answer: `Enter` save, `Shift+Enter` newline
- chat: `Enter` asks the top-level agent

Pressing `x` completes and closes the GrillMe session, discards unanswered questions, and asks the agent to call `dag_grillme_get_answers` to read only `{ id, title, body, answer }` for answered/non-discarded questions. The agent should synthesize those answers into `.ai/project.md`.

Durable state lives in `.ai/grillme/grillme-N.json`. Research summaries and source links should be recorded in `.ai/project.md`.
