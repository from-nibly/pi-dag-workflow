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
  #pendingRefresh = false;
  #drainPromise: Promise<void> | null = null;
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

  start(): void {
    if (this.#disposed || this.#refreshTimer) return;
    this.#refreshTimer = this.#scheduleInterval(() => { void this.refresh(); }, this.#refreshIntervalMs);
    this.#refreshTimer.unref?.();
  }

  refresh(): Promise<void> {
    if (this.#disposed) return Promise.resolve();
    this.#pendingRefresh = true;
    if (!this.#drainPromise) {
      this.#drainPromise = this.#drain().finally(() => {
        this.#drainPromise = null;
        if (this.#pendingRefresh && !this.#disposed) void this.refresh();
      });
    }
    return this.#drainPromise;
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
    this.#pendingRefresh = false;
    if (this.#refreshTimer) this.#clearScheduledInterval(this.#refreshTimer);
    this.#refreshTimer = null;
    this.#projection = null;
    this.#diagnostic = null;
    this.#observedAtMs = null;
    this.#visibleSignature = "disposed";
  }

  async #drain(): Promise<void> {
    while (this.#pendingRefresh && !this.#disposed) {
      this.#pendingRefresh = false;
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
          continue;
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
  }

  #requestRenderIfChanged(): void {
    const signature = JSON.stringify({ projectionHash: this.#projection?.projectionHash ?? null, diagnostic: this.#diagnostic });
    if (signature === this.#visibleSignature) return;
    this.#visibleSignature = signature;
    this.#requestRender();
  }
}
