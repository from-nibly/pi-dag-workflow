import { requireV2 } from "../planning/v2.ts";

/** V2 stored-wire compatibility only. Never mutate persisted history, requests or
 * result bytes: their original hashes and exact-request comparisons remain valid.
 * The removed member is recognized only at explicit authority locations, and is
 * validated before creating a shape-validation view. Public inputs stay strict. */
export function historicalAuthorityViewV2<T extends object>(authority: T): T {
  if (!authority || typeof authority !== "object" || !Object.hasOwn(authority, "expiresAt")) return authority;
  const { expiresAt, ...current } = authority as T & { expiresAt: unknown };
  requireV2(Number.isSafeInteger(expiresAt) && (expiresAt as number) >= 1, "INVALID_HISTORICAL_AUTHORITY");
  return current as T;
}
export function historicalRequestViewV2<T extends { authority: object }>(request: T): T {
  return { ...request, authority: historicalAuthorityViewV2(request.authority) };
}
export function historicalResultViewV2<T extends { request: { authority: object } }>(result: T): T {
  return { ...result, request: historicalRequestViewV2(result.request) };
}
export function historicalSnapshotViewV2(value: unknown): unknown {
  const view = structuredClone(value) as any;
  const execution = (entry: any) => {
    if (entry?.request?.authority) entry.request = historicalRequestViewV2(entry.request);
    if (entry?.result?.request?.authority) entry.result = historicalResultViewV2(entry.result);
    if (entry?.contextRejection?.observed?.request?.authority) entry.contextRejection.observed = historicalResultViewV2(entry.contextRejection.observed);
  };
  for (const run of Object.values(view?.runs ?? {}) as any[]) {
    if (run?.start?.authority) run.start.authority = historicalAuthorityViewV2(run.start.authority);
    for (const node of Object.values(run?.nodes ?? {}) as any[]) {
      if (Array.isArray(node?.lifecycle?.executions)) node.lifecycle.executions.forEach(execution);
    }
    if (Array.isArray(run?.gitOperations)) for (const op of run.gitOperations) {
      if (Array.isArray(op?.checks)) op.checks = op.checks.map((request: any) => request?.authority ? historicalRequestViewV2(request) : request);
    }
  }
  for (const job of Object.values(view?.executions ?? {})) execution(job);
  return view;
}
