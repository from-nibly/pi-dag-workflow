export const WORKER_ACTIVITY_EVENT = "pi-dag-workflow:worker-activity";
export const WORKER_ACTIVITY_REQUEST_EVENT = "pi-dag-workflow:worker-activity-request";

// Subscribe before requesting. Requests replay the manager's committed view;
// they never scan artifacts, acknowledge completions, or wake the parent agent.
export function connectWorkerActivityBus(events, manager) {
  const publish = (snapshot, requestId) => {
    if (!snapshot.reportingEligible) return;
    events.emit(WORKER_ACTIVITY_EVENT, { ...snapshot, ...(requestId === undefined ? {} : { requestId }) });
  };
  const unsubscribeActivity = manager.onActivity(publish);
  const unsubscribeRequest = events.on(WORKER_ACTIVITY_REQUEST_EVENT, (request) => {
    if (request?.schemaVersion !== 1 || typeof request.requestId !== "string" || !request.requestId.length || request.requestId.length > 256 || typeof request.ownerSessionId !== "string") return;
    const snapshot = manager.activitySnapshot();
    if (snapshot.ownerSessionId === request.ownerSessionId) publish(snapshot, request.requestId);
  });
  return () => { unsubscribeRequest(); unsubscribeActivity(); };
}
