# Worker activity bus (v1)

This is a generic, process-local status API, not a HerdR dependency and not a
`pi-subagents` compatibility API. `WorkerManager` is the sole authority. There is
no second artifact scanner, task/tool inference, start/end counter, or model call.

## Channels and handshake

- Producer: `pi-dag-workflow:worker-activity`
- Request: `pi-dag-workflow:worker-activity-request`

A consumer must subscribe **before** requesting, both on session start and after
consumer reload. Emit:

```ts
pi.events.emit("pi-dag-workflow:worker-activity-request", {
  schemaVersion: 1,
  ownerSessionId: ctx.sessionManager.getSessionId(),
  requestId: "consumer-generated-correlation-id",
});
```

`requestId` must be a nonempty string of at most 256 characters. Invalid requests,
unsupported versions, and other owners are ignored. A matching request receives
the current snapshot on the producer channel with the same `requestId`. Responses
are broadcasts; correlation does not imply exclusive delivery. They replay the
cached committed view synchronously, without disk IO or advancing its revision.

If the producer has not attached yet, there may be no response. Its subsequent
attach/recovery publications cover consumer-first load order. If the producer
attached first, the request covers producer-first load order. No response means
unsupported/not-yet-attached, **not** proof that there are no workers.

Only `ctx.mode === "tui"` attachments publish on this bus. JSON, print, and RPC
managers can still execute workers but cannot claim terminal reporting, even if
`ctx.hasUI` is true (RPC). Child workers must not report on the parent's behalf.
Subscriptions are removed at session shutdown after the final detach snapshot.

## Snapshot

```ts
type WorkerActivity = {
  schemaVersion: 1;
  producer: "pi-dag-workflow";
  ownerSessionId: string;
  epoch: number;
  revision: number;
  phase: "attaching" | "attached" | "error" | "detached";
  reportingEligible: true; // bus only; manager API can return false
  working: boolean;
  stores: Array<{
    repositoryRoot: string;
    storageId: string;
    storageRevision: number;
    activeAttempts: Array<{
      workerId: string;
      attemptNumber: number;
      attemptNonce: string | null;
      status: string;
    }>;
    queuedCompletionIds: string[];
    inFlightCompletionId: string | null;
  }>;
  requestId?: string; // only on a handshake response
};
```

Every event is a **full replacement**, not a delta. Arrays may be empty. There are
no tasks, labels, reports, diagnostics, prompts, credentials, or result text.
Storage identity is `(repositoryRoot, storageId)`; it can survive a fork while
`ownerSessionId` changes. `stores` includes the attached store and exact binding
stores visited by this manager's existing ownership/reconciliation paths. Those
paths refresh their committed views; the status API adds no background scanning.

- `epoch` increases process-wide on each attachment, including same-session
  reload. It is not a durable identifier or comparable across Pi processes.
- `revision` increases on each publication within that epoch. Handshake replay
  retains it. `storageRevision` is the durable store's independent CAS revision.
- An active attempt is the **current** attempt of a nonterminal worker.
  Pre-attempt launch reservations appear as `attemptNumber: 0, attemptNonce: null`.
  `launching`, `running`, `settling`, and `cancelling` remain active.
- Terminal worker statuses are `succeeded`, `needs_attention`, `failed`,
  `cancelled`, and `lost`. They are not active attempts. In particular a
  `needs_attention` safety stop is not evidence that its OS process is dead;
  this API describes manager work, not process-liveness/termination authority.
- `working` is true if any store has active attempts, queued completion IDs, or
  an in-flight completion ID. The completion bridge prevents a false idle gap
  between terminal ingestion and the parent's follow-up turn. The manager clears
  in-flight completion on `agent_settled`, not merely on delivery or `agent_end`.
  A late completion for attempt N cannot remove active attempt N+1.
- `attaching` is provisional, not a recovered no-work assertion. `attached` is
  authoritative for the manager's current committed view (external artifact
  changes become visible through its existing scan/reconciliation cadence).
- `error` retains last-known committed work, including undelivered completions.
  It is not proof of idleness. Ownership loss removes that storage's old view.
  A successful scan restores `attached`. Error strings are not exported.
- `detached` invalidates this attachment, with `working: false` and empty stores;
  it does **not** cancel detached workers. A successor recovers them from storage.

Publish points include attach/recovery, every committed manager mutation (launch
reservation, attempt reservation/dispatch, cancel/retry, terminal ingestion,
completion queuing/delivery/acknowledgement), scans, binding-store ownership
transfer, errors, and detach. Consumers must tolerate redundant publications.
Listener exceptions do not fail worker mutations. Manager generation checks fence
queued scans, cancellation escalation, delayed dispatch, and completion delivery
when the attachment changes; stale snapshot revisions cannot regress newer ones.

## Consumer policy (including HerdR)

1. Subscribe during extension setup; on `session_start` set the current owner and
   request its snapshot. Ignore mismatched owners, versions, or producers.
2. Keep the highest accepted `(epoch, revision)` for that owner. Ignore older
   epochs and revisions. Same-revision handshake responses are harmless replays.
   Check ordering **before** applying a detach snapshot. Clear state on actual
   session replacement/shutdown and remove the bus subscription.
3. For a valid attached snapshot, use `foregroundWorking || snapshot.working`.
   Do not reset worker work on parent `agent_settled`; the producer publishes its
   completion acknowledgement separately, possibly after your event handler.
4. Do not turn `attaching`, a missing handshake response, or an `error` without a
   recovered view into an affirmative idle transition. During same-session
   reload, preserve the last known status until the successor attaches; use the
   session lifecycle to distinguish reload from exit. Detach from a retired epoch
   must never clear a newer attachment.
5. Headless consumers must independently gate their terminal reporting to TUI.
   This bus is status only: do not poll observation tools, scan worker storage,
   acknowledge completions, or infer that a task is complete from it.

Manager-level consumers can use `manager.onActivity(listener)` (returns an
unsubscribe function) and `manager.activitySnapshot()` (fresh plain-data copy).
The initial, never-attached manager has epoch 0, null owner, phase `detached`,
empty stores, and `reportingEligible: false`; it is never emitted on the bus.

## Verification

Run `npm run test:worker-activity` for concurrent workers, reservation snapshots,
cancellation pending, retry generation safety, terminal/completion continuity,
load-order handshake, reload recovery, ownership transfer/session switching,
stale dispatch/delivery, headless suppression, no-work/errors, and cleanup.
`npm run test:workers` also runs the existing worker-runtime regression suite.
