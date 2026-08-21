import { truncateToWidth, visibleWidth } from "@earendil-works/pi-tui";
import { canonicalHash } from "./common.ts";
import type { DagExecutionNodeV1, DagExecutionNodeV2, DagExecutionProjectionV1, DagExecutionProjectionV2 } from "./scheduler.ts";

export interface DagWidgetLayoutV1 {
  schemaVersion: 1;
  kind: "DagWidgetLayoutV1";
  projectionHash: string;
  width: number;
  terminalRows: number;
  rowBudget: number;
  observationMinute: string;
  selectedAliases: string[];
  omittedAliases: string[];
  lines: string[];
  layoutHash: string;
}

export function renderDagWidgetV1(projection: DagExecutionProjectionV1, width: number, terminalRows: number, observationTime: string): DagWidgetLayoutV1 {
  if (!Number.isInteger(width) || width < 20 || !Number.isInteger(terminalRows) || terminalRows < 4) throw new TypeError("DAG widget requires integer width >=20 and terminalRows >=4");
  if (!Number.isFinite(Date.parse(observationTime))) throw new TypeError("DAG widget observationTime must be an explicit timestamp");
  const rowBudget = Math.max(4, Math.min(12, Math.floor(terminalRows / 3)));
  const observationMinute = new Date(Math.floor(Date.parse(observationTime) / 60_000) * 60_000).toISOString();
  const selected = selectNodes(projection, Math.max(2, rowBudget - 3));
  const selectedSet = new Set(selected.map(({ alias }) => alias));
  const omitted = projection.nodes.filter(({ alias }) => !selectedSet.has(alias));
  const lines: string[] = [];
  const stale = projection.staleReadOnly ? " STALE READ-ONLY" : "";
  lines.push(crop(`DAG ${projection.runId} r${projection.runRevision}${stale} | ${projection.current}/${projection.completion} | lanes ${projection.summary.activeLanes}/${projection.nodes.length} ready ${projection.summary.ready} attention ${projection.summary.attention}`, width));
  const topologyBudget = Math.max(1, rowBudget - 3);
  const topology = renderTopology(projection, selected, width, topologyBudget); lines.push(...topology.lines);
  const active = selected.filter(({ activeLane }) => activeLane).sort((a, b) => (a.laneAdmissionSequence ?? Number.MAX_SAFE_INTEGER) - (b.laneAdmissionSequence ?? Number.MAX_SAFE_INTEGER) || a.alias.localeCompare(b.alias));
  const omittedActive = omitted.filter(({ activeLane }) => activeLane).map(({ alias }) => alias);
  const omittedIds = new Set(omitted.map(({ workItemId }) => workItemId)); const selectedIds = new Set(selected.map(({ workItemId }) => workItemId)); const upstream = projection.precedence.filter(({ from, to }) => omittedIds.has(from) && selectedIds.has(to)).length; const downstream = projection.precedence.filter(({ from, to }) => selectedIds.has(from) && omittedIds.has(to)).length;
  const cutRequired = omitted.length > 0 || topology.omittedRows > 0;
  if (lines.length < rowBudget - (cutRequired ? 2 : 1)) lines.push(crop(active.length ? `active: ${active.map((node) => `${node.alias}${node.glyph}${node.stage ? `/${node.stage}` : ""}${node.worker ? `:${node.worker.workerId}` : ""}${node.admittedAt ? ` ${elapsedMinutes(node.admittedAt, observationMinute)}` : ""}`).join(" ")}` : "active: none", width));
  if (cutRequired) lines.push(crop(`omitted: ${omitted.length} nodes; topology ${topology.omittedRows} rows cut; edges ${topology.omittedEdges} cut; upstream ${upstream}; downstream ${downstream}${omittedActive.length ? `; active ${omittedActive.join(",")}` : ""}${omitted.length ? ` | states ${stateCounts(omitted)}` : ""}`, width));
  while (lines.length > rowBudget - 1) lines.splice(lines.length - (cutRequired ? 1 : 0) - 1, 1);
  lines.push(crop("read-only; ask the conductor to inspect/control this run", width));
  const core = {
    schemaVersion: 1 as const, kind: "DagWidgetLayoutV1" as const, projectionHash: projection.projectionHash,
    width, terminalRows, rowBudget, observationMinute, selectedAliases: selected.map(({ alias }) => alias).sort(), omittedAliases: omitted.map(({ alias }) => alias).sort(), lines,
  };
  return { ...core, layoutHash: canonicalHash(core) };
}

export interface DagWidgetRenderStateV2 {
  animationFrame: number;
  diagnostic?: string | null;
  freshLiveAliases: readonly string[];
}

export interface DagWidgetLayoutV2 {
  schemaVersion: 2;
  kind: "DagWidgetLayoutV2";
  projectionHash: string;
  width: number;
  terminalRows: number;
  rowBudget: number;
  activityAliases: string[];
  selectedAliases: string[];
  omittedAliases: string[];
  omittedEdges: number;
  lines: string[];
  layoutHash: string;
}

export function renderDagWidgetV2(projection: DagExecutionProjectionV2, width: number, terminalRows: number, observationTime: string, view: DagWidgetRenderStateV2 = { animationFrame: 0, freshLiveAliases: [] }): DagWidgetLayoutV2 {
  if (!Number.isInteger(width) || width < 1 || !Number.isInteger(terminalRows) || terminalRows < 4) throw new TypeError("DAG widget requires integer width >=1 and terminalRows >=4");
  if (!Number.isFinite(Date.parse(observationTime))) throw new TypeError("DAG widget observationTime must be an explicit timestamp");
  const rowBudget = Math.max(4, Math.min(12, Math.floor(terminalRows / 3)));
  const byId = new Map(projection.nodes.map((node) => [node.workItemId, node]));
  const anchors = selectAnchorsV2(projection);
  const lines: string[] = [];
  const activity = new Set<string>();
  const selected = new Set<string>();
  let shownEdges = 0;
  const stale = projection.staleReadOnly || view.diagnostic ? " | STALE" : "";
  lines.push(crop(`DAG ${projection.runId} r${projection.runRevision} | ${projection.current}/${projection.completion} | active ${projection.summary.activeLanes}/${projection.nodes.length} ready ${projection.summary.ready} attention ${projection.summary.attention}${stale}`, width));

  const contentBudget = rowBudget - 2;
  if (width < 20) {
    for (const node of anchors.slice(0, contentBudget)) {
      selected.add(node.alias);
      lines.push(crop(`${node.glyph}${node.alias} ${node.stage ?? "-"}`, width));
    }
  } else {
    let used = 0;
    for (const anchor of anchors) {
      const dependents = outgoingDependentsV2(projection, anchor, byId);
      const block = renderGraphBranchV2(anchor, dependents, width);
      if (used + block.lines.length > contentBudget) {
        if (used === 0) {
          activity.add(anchor.alias);
          selected.add(anchor.alias);
          lines.push(renderAnchorV2(anchor, width));
          used += 1;
          if (used < contentBudget && dependents.length) {
            const compact = renderDependentRowV2(dependents, width);
            lines.push(compact.line);
            compact.visible.forEach(({ alias }) => selected.add(alias));
            shownEdges += compact.visible.length;
            used += 1;
          }
        }
        break;
      }
      activity.add(anchor.alias);
      selected.add(anchor.alias);
      block.visibleDependents.forEach(({ alias }) => selected.add(alias));
      shownEdges += block.visibleDependents.length;
      lines.push(...block.lines);
      used += block.lines.length;
    }
  }

  const omittedAliases = projection.nodes.map(({ alias }) => alias).filter((alias) => !selected.has(alias)).sort();
  const omittedEdges = Math.max(0, projection.precedence.length - shownEdges);
  const omission = omittedAliases.length || omittedEdges ? `+${omittedAliases.length} nodes · +${omittedEdges} edges omitted` : null;
  const staleDiagnostic = view.diagnostic ?? (projection.staleReadOnly ? "STALE READ-ONLY" : null);
  const footer = staleDiagnostic
    ? `${staleDiagnostic}${omission ? ` | ${omission}` : ""} | read-only`
    : omission
      ? `${omission} | read-only`
      : "read-only; inspect/control through the conductor";
  while (lines.length > rowBudget - 1) lines.pop();
  lines.push(crop(footer, width));
  const core = {
    schemaVersion: 2 as const,
    kind: "DagWidgetLayoutV2" as const,
    projectionHash: projection.projectionHash,
    width,
    terminalRows,
    rowBudget,
    activityAliases: [...activity].sort(),
    selectedAliases: [...selected].sort(),
    omittedAliases,
    omittedEdges,
    lines,
  };
  return { ...core, layoutHash: canonicalHash(core) };
}

function selectAnchorsV2(projection: DagExecutionProjectionV2): DagExecutionNodeV2[] {
  const selected = new Map<string, DagExecutionNodeV2>();
  const add = (node: DagExecutionNodeV2 | undefined) => { if (node) selected.set(node.workItemId, node); };
  projection.nodes.filter(({ glyph }) => glyph === "!" || glyph === "?").sort(attentionOrderV2).forEach(add);
  projection.nodes.filter(({ activeLane }) => activeLane).sort(laneOrderV2).forEach(add);
  if (selected.size === 0) {
    const byId = new Map(projection.nodes.map((node) => [node.workItemId, node]));
    projection.trainHeads.forEach(({ workItemId }) => add(workItemId ? byId.get(workItemId) : undefined));
    projection.nodes.filter(({ correctnessReady }) => correctnessReady).sort(schedulerOrderV2).forEach(add);
  }
  if (selected.size === 0) add(projection.nodes.filter(({ glyph }) => !["#", "x"].includes(glyph)).sort((a, b) => a.alias.localeCompare(b.alias))[0]);
  return [...selected.values()];
}

function outgoingDependentsV2(projection: DagExecutionProjectionV2, anchor: DagExecutionNodeV2, byId: Map<string, DagExecutionNodeV2>): DagExecutionNodeV2[] {
  return projection.precedence
    .filter(({ from }) => from === anchor.workItemId)
    .map(({ to }) => byId.get(to))
    .filter((node): node is DagExecutionNodeV2 => Boolean(node))
    .sort(dependentOrderV2);
}

function renderGraphBranchV2(anchor: DagExecutionNodeV2, dependents: DagExecutionNodeV2[], width: number): { lines: string[]; visibleDependents: DagExecutionNodeV2[] } {
  const anchorLine = renderAnchorV2(anchor, width);
  if (dependents.length === 0) return { lines: [anchorLine], visibleDependents: [] };
  const dependentRow = renderDependentRowV2(dependents, width);
  if (dependentRow.visible.length === 0) return { lines: [anchorLine], visibleDependents: [] };
  const sourceColumn = Math.min(width - 1, 3);
  const connector = Array(width).fill(" ");
  connector[sourceColumn] = "└";
  const lastPosition = dependentRow.positions.at(-1)!;
  for (let column = sourceColumn + 1; column <= lastPosition; column += 1) connector[column] = "─";
  dependentRow.positions.forEach((column, index) => { connector[column] = index === dependentRow.positions.length - 1 ? "┐" : "┬"; });
  return { lines: [anchorLine, connector.join("").trimEnd(), dependentRow.line], visibleDependents: dependentRow.visible };
}

function renderAnchorV2(node: DagExecutionNodeV2, width: number): string {
  const motion = node.worker?.terminalStatus === null ? "·" : " ";
  const progress = renderProgressV2(node, width);
  const prefix = `${motion} ${node.glyph}${node.alias} ${progress}`;
  const available = Math.max(0, width - visibleWidth(prefix) - 1);
  return crop(available > 0 ? `${prefix} ${shortTitle(node.title, available)}` : prefix, width);
}

function renderProgressV2(node: DagExecutionNodeV2, width: number): string {
  const passed = node.stages.filter(({ state }) => state === "passed").length;
  if (width < 60) return `${node.stage ?? "-"} ${passed}/9`;
  const cells = node.stages.map(({ stage, state }) => state === "passed" ? "■" : stage === node.stage ? "▶" : "·").join("");
  return `${node.stage ?? "-"} [${cells}]`;
}

function renderDependentRowV2(dependents: DagExecutionNodeV2[], width: number): { line: string; positions: number[]; visible: DagExecutionNodeV2[] } {
  const visible: DagExecutionNodeV2[] = [];
  let labelsWidth = 0;
  for (const node of dependents) {
    const labelWidth = visibleWidth(`▶ ${node.alias}`);
    const nextWidth = labelsWidth + (visible.length ? 1 : 0) + labelWidth;
    if (nextWidth > Math.max(0, width - 4)) break;
    visible.push(node);
    labelsWidth = nextWidth;
  }
  if (visible.length === 0) return { line: "", positions: [], visible };
  const labelWidths = visible.map(({ alias }) => visibleWidth(`▶ ${alias}`));
  const lastColumn = width - labelWidths.at(-1)!;
  const packedFirstColumn = lastColumn - labelWidths.slice(0, -1).reduce((sum, labelWidth) => sum + labelWidth + 1, 0);
  const desiredFirstColumn = Math.max(4, Math.floor(width * 0.38));
  const firstColumn = Math.max(4, Math.min(desiredFirstColumn, packedFirstColumn));
  const positions = visible.map((_, index) => visible.length === 1 ? lastColumn : Math.round(firstColumn + (lastColumn - firstColumn) * index / (visible.length - 1)));
  const row = Array(width).fill(" ");
  visible.forEach((node, index) => Array.from(`▶ ${node.alias}`).forEach((character, offset) => { if (positions[index] + offset < width) row[positions[index] + offset] = character; }));
  return { line: row.join("").trimEnd(), positions, visible };
}

function laneOrderV2(a: DagExecutionNodeV2, b: DagExecutionNodeV2): number {
  return (a.laneAdmissionSequence ?? Number.MAX_SAFE_INTEGER) - (b.laneAdmissionSequence ?? Number.MAX_SAFE_INTEGER) || a.alias.localeCompare(b.alias);
}
function attentionOrderV2(a: DagExecutionNodeV2, b: DagExecutionNodeV2): number { return Number(b.glyph === "!") - Number(a.glyph === "!") || laneOrderV2(a, b); }
function schedulerOrderV2(a: DagExecutionNodeV2, b: DagExecutionNodeV2): number { return (a.schedulerOrder ?? Number.MAX_SAFE_INTEGER) - (b.schedulerOrder ?? Number.MAX_SAFE_INTEGER) || a.alias.localeCompare(b.alias); }
function dependentOrderV2(a: DagExecutionNodeV2, b: DagExecutionNodeV2): number { return Number(b.glyph === "!" || b.glyph === "?") - Number(a.glyph === "!" || a.glyph === "?") || Number(b.activeLane) - Number(a.activeLane) || schedulerOrderV2(a, b); }

function selectNodes(projection: DagExecutionProjectionV1, limit: number): DagExecutionNodeV1[] {
  const byId = new Map(projection.nodes.map((node) => [node.workItemId, node]));
  const selected = new Map<string, DagExecutionNodeV1>();
  const add = (node: DagExecutionNodeV1 | undefined) => { if (node && selected.size < limit) selected.set(node.workItemId, node); };
  projection.nodes.filter(({ glyph }) => glyph === "!" || glyph === "?").sort(attentionOrder).forEach(add);
  projection.nodes.filter(({ activeLane }) => activeLane).sort((a, b) => (a.laneAdmissionSequence ?? Number.MAX_SAFE_INTEGER) - (b.laneAdmissionSequence ?? Number.MAX_SAFE_INTEGER) || a.alias.localeCompare(b.alias)).forEach(add);
  for (const head of projection.trainHeads) add(head.workItemId ? byId.get(head.workItemId) : undefined);
  projection.nodes.filter(({ correctnessReady }) => correctnessReady).sort((a, b) => (a.schedulerOrder ?? Number.MAX_SAFE_INTEGER) - (b.schedulerOrder ?? Number.MAX_SAFE_INTEGER) || a.alias.localeCompare(b.alias)).forEach(add);
  const anchors = [...selected.values()];
  for (const edge of projection.precedence) if (anchors.some(({ workItemId }) => workItemId === edge.from || workItemId === edge.to)) { add(byId.get(edge.from)); add(byId.get(edge.to)); }
  projection.nodes.sort((a, b) => a.alias.localeCompare(b.alias)).forEach(add);
  return [...selected.values()].sort((a, b) => a.alias.localeCompare(b.alias));
}

function renderTopology(projection: DagExecutionProjectionV1, selected: DagExecutionNodeV1[], width: number, budget: number): { lines: string[]; omittedRows: number; omittedEdges: number } {
  const selectedIds = new Set(selected.map(({ workItemId }) => workItemId));
  const edges = projection.precedence.filter(({ from, to }) => selectedIds.has(from) && selectedIds.has(to));
  if (width < 60) {
    const candidates = selected.map((node) => {
      const predecessors = edges.filter(({ to }) => to === node.workItemId).map(({ from }) => selected.find(({ workItemId }) => workItemId === from)!.alias).sort();
      const successors = edges.filter(({ from }) => from === node.workItemId).map(({ to }) => selected.find(({ workItemId }) => workItemId === to)!.alias).sort();
      return crop(`${node.alias}${node.glyph} <- ${predecessors.join(",") || "-"} -> ${successors.join(",") || "-"} ${shortTitle(node.title, Math.max(0, width - 25))}`, width);
    }); return { lines: candidates.slice(0, budget), omittedRows: Math.max(0, candidates.length - budget), omittedEdges: 0 };
  }
  const label = (id: string) => { const node = selected.find(({ workItemId }) => workItemId === id)!; return `${node.alias}${node.glyph}${width >= 96 ? ` ${shortTitle(node.title, 14)}` : ""}`; };
  const edgeLines = edges.slice().sort((a, b) => a.from.localeCompare(b.from) || a.to.localeCompare(b.to)).map(({ from, to }) => `${label(from)} -> ${label(to)}`);
  const connected = new Set(edges.flatMap(({ from, to }) => [from, to])); const isolated = selected.filter(({ workItemId }) => !connected.has(workItemId)).map(({ workItemId }) => label(workItemId));
  const candidates = [...edgeLines, ...isolated]; return { lines: candidates.slice(0, budget).map((line) => crop(line, width)), omittedRows: Math.max(0, candidates.length - budget), omittedEdges: Math.max(0, edgeLines.length - budget) };
}

function buildChains(nodes: DagExecutionNodeV1[], edges: DagExecutionProjectionV1["precedence"]): string[][] {
  const nodeIds = new Set(nodes.map(({ workItemId }) => workItemId));
  const incoming = new Map(nodes.map(({ workItemId }) => [workItemId, 0]));
  for (const edge of edges) incoming.set(edge.to, (incoming.get(edge.to) ?? 0) + 1);
  const starts = nodes.filter(({ workItemId }) => (incoming.get(workItemId) ?? 0) === 0 && edges.some(({ from }) => from === workItemId)).sort((a, b) => a.alias.localeCompare(b.alias));
  const chains: string[][] = [];
  for (const start of starts) {
    const chain = [start.workItemId]; let current = start.workItemId; const seen = new Set(chain);
    while (true) {
      const next = edges.filter(({ from, to }) => from === current && nodeIds.has(to) && !seen.has(to)).map(({ to }) => to).sort()[0];
      if (!next) break; chain.push(next); seen.add(next); current = next;
    }
    chains.push(chain);
  }
  return chains;
}

function stateCounts(nodes: DagExecutionNodeV1[]): string { const counts = new Map<string, number>(); for (const node of nodes) counts.set(node.glyph, (counts.get(node.glyph) ?? 0) + 1); return [...counts.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([glyph, count]) => `${glyph}${count}`).join(" "); }
function attentionOrder(a: DagExecutionNodeV1, b: DagExecutionNodeV1): number { return Number(b.glyph === "!") - Number(a.glyph === "!") || a.alias.localeCompare(b.alias); }
function shortTitle(value: string, width: number): string { if (width <= 0) return ""; return visibleWidth(value) <= width ? value : truncateToWidth(value, width, "…"); }
function crop(value: string, width: number): string { return visibleWidth(value) <= width ? value : truncateToWidth(value, width, "…"); }
function elapsedMinutes(startedAt: string, observationMinute: string): string { const minutes = Math.max(0, Math.floor((Date.parse(observationMinute) - Date.parse(startedAt)) / 60_000)); return `${minutes}m`; }
