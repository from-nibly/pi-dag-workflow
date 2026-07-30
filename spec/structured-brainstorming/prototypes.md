# Prototype policy

> **Migration evidence:** The explicit-request and behavioral-reference rules remain active; any legacy promotion or hand-maintained-spec authority assumptions are superseded by the [mixed-initiative project model](../mixed-initiative-project-model/spec.md).

The agent creates a prototype only after the user explicitly requests one. It may recommend a prototype when tangible behavior would clarify the discussion, but safe local experimentation is not automatic prototype creation.

Every requested prototype starts under:

```text
spec/prototypes/<easy-reference-slug>/
```

Prototype slug directories are the exception to the universal `spec.md` rule. Each contains `README.md` instead.

## Behavioral-reference contract

A prototype is a bounded behavioral reference, not rigid code-level authority. Its primary purpose is to demonstrate and codify selected behavior for planning and implementation judgment. Its secondary purpose is to provide something runnable or viewable that exposes flaws in the design quickly.

Each README states:

- why the prototype exists;
- how to run or view it;
- expected observations;
- behavior demonstrated;
- findings produced so far;
- behavior outside its scope;
- where its definitions end;
- important omitted cases in a partial sample;
- links to related canonical specs.

**Rationale:** concrete behavior helps implementation agents, but prototypes often contain fake data, shortcuts, partial flows, and accidental details.

## Ambiguity

If prototype behavior and canonical Markdown appear inconsistent, the agent surfaces the conflict to the user immediately. Neither artifact silently wins. The user determines how to resolve or explicitly dispose the ambiguity; the tool does not create an automatic planning gate.

## Planning and chunking

Prototypes are optional context at the workflow level. Planning decides which plan sections reference them. Chunking decides which referenced prototypes are required reading for individual nodes or flow steps. Agents use judgment within each README's stated boundaries.

## Curation

Prototype files may be changed freely during brainstorming. Retain a prototype while it improves behavioral understanding or validation. It may be removed when its unique role is gone and important findings or contracts are preserved elsewhere.

Before deleting a failed, superseded, or noisy artifact, preserve any finding that still affects the specification.

**Rationale:** `spec/prototypes/` should remain a useful behavioral library, not an append-only experiment archive.
