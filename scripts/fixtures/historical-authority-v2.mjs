// Frozen pre-removal V2 wire producer for compatibility regressions. These fields
// are present at initial publication, before reservation/config/evidence hashes
// exist. Never retrofit them onto a current run or rewrite old evidence hashes.
import { randomUUID } from 'node:crypto';
import { selectorV2 } from '../../extensions/dag-workflow/planning/v2.ts';

export async function historicalStartV2(runtime, plan, sessionId, expiresAt) {
  const authority = { scope: plan.workItems.map(n => n.id), maxConcurrency: 1, effects: ['repository_local'], expiresAt };
  const acceptance = await runtime.assess(selectorV2(plan), { scope: authority.scope, maxConcurrency: authority.maxConcurrency, effects: authority.effects });
  return runtime.store.transaction(async (s, publish) => {
    if (s.bindings[sessionId]) throw Error('historical fixture requires an unbound session');
    const run = { kind: 'dag_run_v2', schemaVersion: 2, runId: `run-${randomUUID()}`, revision: 0,
      start: { intent: 'run', sessionId, selection: selectorV2(plan), authority }, acceptance, status: 'active', releasedGates: [],
      nodes: Object.fromEntries(plan.workItems.map(n => [n.id, { generation: 1, status: 'pending' }])) };
    s.runs[run.runId] = run; s.bindings[sessionId] = run.runId;
    await publish(); return run;
  });
}

// Original executionRequestV2 serialization; retained requests/results must use
// these bytes, not a newly computed hash of a normalized request.
export function historicalExecutionRequestV2(run, itemId, check) {
  const n = run.nodes[itemId], l = n.lifecycle;
  return { id: `execution-${randomUUID()}`, plan: structuredClone(run.start.selection), runId: run.runId, itemId, generation: n.generation,
    attempt: `${n.reservation.operationId}/F${l.stage}/${l.round}`, round: l.round, stage: l.stage, candidate: structuredClone(l.candidate),
    implementationWorkerId: n.reservation.workerId, check: structuredClone(check), authority: { effect: 'repository_local', expiresAt: run.start.authority.expiresAt } };
}
export const historicalDeadlineV2 = run => run.start.authority.expiresAt;
export const removedAuthorityFieldV2 = 'expiresAt';
