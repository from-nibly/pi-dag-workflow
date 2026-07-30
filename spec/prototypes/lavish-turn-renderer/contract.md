# Prototype contract

## Purpose

Prove the concrete seams of the accepted whole-turn Lavish renderer without registering a production Pi tool or writing project-model semantics.

## Projection

`ModelReviewTurnProjection` schema version 1 contains only a narrow presentation projection:

- project identity, revision, and model hash;
- active focus identity and selected workstreams;
- non-authoritative Current understanding;
- selected model-delta counts, possible misunderstanding, and consequences;
- selected unresolved frontier cards plus a plain-language post-turn handoff;
- one exact active review packet with hash-bound points and options whose description and rationale contain all prose needed for human judgment;
- optional presentation blocks placed before the review, after the review, or inside one review point.

The projection may retain semantic direction objects for downstream resolution, but the human shell does not expose serialized authority payloads. Standard controls queue IDs and freshness hashes. The shell renders any number of decision points as independent forms; every decision includes an explicit **Other** radio path plus a separate response box that remains available for suggested options too. Lavish's host window remains the primary send surface.

The renderer never reads `project-model/model.json`. The fixture stands in for the future domain projection.

## Presentation blocks

The bounded helper vocabulary is `markdown`, `callout`, `code`, `table`, `comparison`, and trusted `html`. Standard model content is escaped. Trusted HTML is deliberately inserted without sanitization or script isolation under the accepted local threat model.

## Artifact lifecycle

The canonical relative path is:

```text
.ai/model-sessions/<focus-id>/lavish/<review-id>.html
```

The adjacent `<review-id>.presentation.json` records disposable status: `rendered`, `open`, `feedback`, `interrupted`, `user_ended`, or `ended`. The path survives interrupted polling. Resolution cleanup best-effort ends Lavish and removes both files.

## Feedback boundary

The CLI adapter recognizes only the bounded public AXI/TOON result fields needed by the agent:

- session status, end flag, and end actor;
- prompt `uid`, `prompt`, `selector`, `tag`, `text`, and target;
- severe layout-warning fields.

It drops DOM snapshots and `next_step`, caps prompt count, caps each prompt, caps total returned bytes, and reports truncation explicitly. Lavish feedback is assumed human-initiated, but review, point, option, and semantic hashes still protect freshness and routing before the semantic resolver runs.

## Non-goals

- Production extension registration.
- Project-model reads or writes.
- Automatic semantic outcome resolution.
- A JavaScript API for Lavish.
- Permanent oversight-report storage.
- Final visual branding.
