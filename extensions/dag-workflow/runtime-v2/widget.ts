import { truncateToWidth } from "@earendil-works/pi-tui";
import type { ProductV2 } from "./product.ts";

/** Passive session-scoped view; no worker reconciliation or writer acquisition.
 * The revision is presentation metadata, never a fabricated V1 authority hash. */
export class ProductWidgetV2 {
  private epoch = 0;
  private pending?: Promise<void>;
  private timer?: ReturnType<typeof setInterval>;
  private last = "";
  private good?: { runId: string; lines: string[] };
  private context: any;
  readonly getProduct: (ctx: any) => ProductV2;
  constructor(getProduct: (ctx: any) => ProductV2) { this.getProduct = getProduct; }
  mount(ctx: any) {
    this.dispose();
    if (!ctx.hasUI || ctx.mode !== "tui" || typeof ctx.ui?.setWidget !== "function") return;
    this.context = ctx;
    this.timer = setInterval(() => { void this.refresh(); }, 1500); this.timer.unref?.();
    void this.refresh();
  }
  async refresh() {
    if (!this.context) return;
    if (this.pending) return this.pending;
    const ctx = this.context, epoch = this.epoch;
    const update = async () => {
      let lines: string[] = [], runId: string | undefined;
      try {
        const { run, plan } = await this.getProduct(ctx).read(); runId = run?.runId;
        if (run && plan) {
          lines = [`DAG V2 ${plan.title} · ${run.status} · r${run.revision}`,
            ...plan.workItems.slice(0, 12).map(item => { const n = run.nodes[item.id]; return `${item.id} ${n.status} g${n.generation}${n.lifecycle ? ` F${n.lifecycle.stage}${n.lifecycle.stop ? " STOP" : ""}` : ""}${item.dependsOn.length ? ` ← ${item.dependsOn.join(",")}` : ""}`; })];
          if (plan.workItems.length > 12) lines.push(`… ${plan.workItems.length - 12} more; /dag show`);
        }
        if (epoch !== this.epoch) return;
        this.good = runId ? { runId, lines } : undefined;
      } catch (error) {
        if (epoch !== this.epoch) return;
        // Last good is scoped to this exact session mount. Do not claim freshness.
        lines = [...(this.good?.lines ?? []), `DAG view stale${this.good ? ` (last observed ${this.good.runId})` : ""}: ${String(error).slice(0, 180)}`];
      }
      const signature = JSON.stringify(lines);
      if (epoch !== this.epoch || signature === this.last) return;
      try {
        ctx.ui.setWidget("canonical-dag-run", lines.length ? () => ({ render: (width: number) => lines.map(line => truncateToWidth(line.replace(/[\x00-\x1f\x7f-\x9f]/g, " "), Math.max(0, width))), invalidate() {} }) : undefined);
        this.last = signature;
      } catch { /* Presentation failure cannot change or fail a committed operation. */ }
    };
    const pending = update().finally(() => { if (this.pending === pending) this.pending = undefined; });
    this.pending = pending; return pending;
  }
  dispose() {
    this.epoch++; if (this.timer) clearInterval(this.timer); this.timer = undefined;
    try { this.context?.ui?.setWidget?.("canonical-dag-run", undefined); } catch { /* The previous host UI may already be disposed. */ }
    this.context = undefined; this.good = undefined; this.last = ""; this.pending = undefined;
  }
}
