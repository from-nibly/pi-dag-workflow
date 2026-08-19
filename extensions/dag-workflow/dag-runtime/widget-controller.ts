import type { DagExecutionProjectionV2 } from "./scheduler.ts";

export const DAG_WIDGET_REFRESH_INTERVAL_MS_V2 = 1_000;
export const DAG_WIDGET_LIVENESS_FRESHNESS_MS_V2 = 2_500;

export type DagWidgetControllerReadResultV2 =
  | { kind: "empty" }
  | { kind: "terminal" }
  | { kind: "projection"; projection: DagExecutionProjectionV2; fresh: boolean; diagnostic: string | null };

export interface DagWidgetViewStateV2 {
  animationFrame: number;
  diagnostic: string | null;
  freshLiveAliases: string[];
  observedAt: string | null;
  projection: DagExecutionProjectionV2 | null;
}

export interface DagWidgetControllerOptionsV2 {
  read: () => Promise<DagWidgetControllerReadResultV2>;
  requestRender: () => void;
  onTerminal?: () => void;
  now?: () => number;
  scheduleInterval?: typeof setInterval;
  clearScheduledInterval?: typeof clearInterval;
  refreshIntervalMs?: number;
  freshnessMs?: number;
  animationIntervalMs?: number;
}

export class DagWidgetControllerV2 {
  readonly #read: DagWidgetControllerOptionsV2["read"];
  readonly #requestRender: () => void;
  readonly #onTerminal: () => void;
  readonly #now: () => number;
  readonly #scheduleInterval: typeof setInterval;
  readonly #clearScheduledInterval: typeof clearInterval;
  readonly #refreshIntervalMs: number;
  #disposed = false;
  #drainPromise: Promise<void> | null = null;
  #queuedRefresh: { promise: Promise<void>; resolve: () => void } | null = null;
  #outstandingQueuedRefreshes = new Set<{ promise: Promise<void>; resolve: () => void }>();
  #startPromise: Promise<void> | null = null;
  #refreshTimer: ReturnType<typeof setInterval> | null = null;
  #projection: DagExecutionProjectionV2 | null = null;
  #diagnostic: string | null = null;
  #observedAtMs: number | null = null;
  #visibleSignature = "uninitialized";

  constructor(options: DagWidgetControllerOptionsV2) {
    this.#read = options.read;
    this.#requestRender = options.requestRender;
    this.#onTerminal = options.onTerminal ?? (() => {});
    this.#now = options.now ?? Date.now;
    this.#scheduleInterval = options.scheduleInterval ?? setInterval;
    this.#clearScheduledInterval = options.clearScheduledInterval ?? clearInterval;
    this.#refreshIntervalMs = options.refreshIntervalMs ?? DAG_WIDGET_REFRESH_INTERVAL_MS_V2;
  }

  start(): Promise<void> {
    if (this.#disposed) return Promise.resolve();
    if (this.#startPromise) return this.#startPromise;
    this.#startPromise = this.refresh().finally(() => {
      if (this.#disposed || this.#refreshTimer) return;
      this.#refreshTimer = this.#scheduleInterval(() => { void this.refresh(); }, this.#refreshIntervalMs);
      this.#refreshTimer.unref?.();
    });
    return this.#startPromise;
  }

  refresh(): Promise<void> {
    if (this.#disposed) return Promise.resolve();
    if (!this.#drainPromise) return this.#startRead();
    if (!this.#queuedRefresh) {
      let resolve!: () => void;
      const promise = new Promise<void>((settle) => { resolve = settle; });
      this.#queuedRefresh = { promise, resolve };
      this.#outstandingQueuedRefreshes.add(this.#queuedRefresh);
    }
    return this.#queuedRefresh.promise;
  }

  #startRead(): Promise<void> {
    const operation = this.#readOnce().finally(() => {
      if (this.#drainPromise === operation) this.#drainPromise = null;
      const queued = this.#queuedRefresh;
      this.#queuedRefresh = null;
      if (!queued) return;
      if (this.#disposed) this.#settleQueuedRefresh(queued);
      else void this.#startRead().then(
        () => this.#settleQueuedRefresh(queued),
        () => this.#settleQueuedRefresh(queued),
      );
    });
    this.#drainPromise = operation;
    return operation;
  }

  snapshot(): DagWidgetViewStateV2 {
    return {
      animationFrame: 0,
      diagnostic: this.#diagnostic,
      freshLiveAliases: [],
      observedAt: this.#observedAtMs === null ? null : new Date(this.#observedAtMs).toISOString(),
      projection: this.#projection,
    };
  }

  noteSelectedAliases(_aliases: string[]): void {}

  failClosed(message: string): void {
    if (this.#disposed) return;
    this.#projection = null;
    this.#diagnostic = message;
    this.#observedAtMs = null;
    this.#requestRenderIfChanged();
  }

  dispose(): void {
    if (this.#disposed) return;
    this.#disposed = true;
    for (const queued of this.#outstandingQueuedRefreshes) queued.resolve();
    this.#outstandingQueuedRefreshes.clear();
    this.#queuedRefresh = null;
    if (this.#refreshTimer) this.#clearScheduledInterval(this.#refreshTimer);
    this.#refreshTimer = null;
    this.#projection = null;
    this.#diagnostic = null;
    this.#observedAtMs = null;
    this.#visibleSignature = "disposed";
  }

  async #readOnce(): Promise<void> {
    if (this.#disposed) return;
    try {
      const result = await this.#read();
      if (this.#disposed) return;
      if (result.kind === "terminal") {
        this.dispose();
        this.#onTerminal();
        return;
      }
      if (result.kind === "empty") {
        this.#projection = null;
        this.#diagnostic = null;
        this.#observedAtMs = null;
        this.#requestRenderIfChanged();
        return;
      }
      this.#projection = result.projection;
      this.#diagnostic = result.diagnostic;
      this.#observedAtMs = result.fresh ? this.#now() : null;
      this.#requestRenderIfChanged();
    } catch (error) {
      if (this.#disposed) return;
      this.failClosed(`DAG projection unavailable: ${String((error as Error).message).slice(0, 160)}`);
    }
  }

  #settleQueuedRefresh(queued: { promise: Promise<void>; resolve: () => void }): void {
    if (!this.#outstandingQueuedRefreshes.delete(queued)) return;
    queued.resolve();
  }

  #requestRenderIfChanged(): void {
    const signature = JSON.stringify({ projectionHash: this.#projection?.projectionHash ?? null, diagnostic: this.#diagnostic });
    if (signature === this.#visibleSignature) return;
    this.#visibleSignature = signature;
    try { this.#requestRender(); }
    catch (error) {
      this.#projection = null;
      this.#diagnostic = `DAG widget render request failed: ${String((error as Error).message).slice(0, 120)}`;
      this.#observedAtMs = null;
      this.#visibleSignature = JSON.stringify({ projectionHash: null, diagnostic: this.#diagnostic });
    }
  }
}
