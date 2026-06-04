import type { DagFile, DagNode } from "./types.ts";

export interface DagDiagramResult {
  text: string;
  warnings: string[];
}

interface GraphInfo {
  nodeIds: Set<string>;
  orderById: Map<string, number>;
  depsById: Map<string, string[]>;
  knownDepsById: Map<string, string[]>;
  childrenById: Map<string, string[]>;
  topoOrder: string[];
  cycleIds: string[];
}

const DEFAULT_WIDTH = 80;
const CHAIN_ARROW = " ──> ";
const HARD_EDGE_TYPES = new Set<string | undefined>([undefined, "hard", "merge-order"]);

export function renderDagDiagram(dag: DagFile, options?: { width?: number }): DagDiagramResult {
  const warnings: string[] = [];
  const width = normalizeWidth(options?.width);
  const nodes = Array.isArray(dag.nodes) ? dag.nodes : [];
  const graph = buildGraph(nodes, warnings);

  warnForLongIds([...graph.nodeIds, ...allDependencyIds(graph.depsById)], width, warnings);
  warnForHardEdgeMismatches(dag, graph.depsById, warnings);

  const lines: string[] = [];
  lines.push(`DAG: ${dag.run?.name ?? "(unnamed)"}`);
  lines.push(`Nodes: ${nodes.length}`);
  lines.push("");
  lines.push(...renderDependencyRows(nodes, graph.depsById, width));
  lines.push("");
  lines.push("Dependency sketch:");
  lines.push(...renderSketch(nodes, graph, width));
  lines.push("");
  lines.push(`First ready: ${renderFirstReady(nodes, graph.depsById)}`);
  lines.push(`maxConcurrency: ${dag.run?.maxConcurrency ?? "(not set)"}`);

  return { text: lines.join("\n"), warnings };
}

function normalizeWidth(width: number | undefined): number {
  return Number.isInteger(width) && width > 0 ? width : DEFAULT_WIDTH;
}

function buildGraph(nodes: DagNode[], warnings: string[]): GraphInfo {
  const nodeIds = new Set<string>();
  const orderById = new Map<string, number>();
  const depsById = new Map<string, string[]>();
  const knownDepsById = new Map<string, string[]>();
  const childrenById = new Map<string, string[]>();

  for (const [index, node] of nodes.entries()) {
    if (nodeIds.has(node.id)) {
      warnings.push(`duplicate node id: ${node.id}`);
      continue;
    }
    nodeIds.add(node.id);
    orderById.set(node.id, index);
    childrenById.set(node.id, []);
  }

  for (const node of nodes) {
    const deps = Array.isArray(node.dependsOn) ? node.dependsOn : [];
    depsById.set(node.id, deps);

    const knownDeps: string[] = [];
    const seenKnownDeps = new Set<string>();
    for (const dep of deps) {
      if (!nodeIds.has(dep)) {
        warnings.push(`node ${node.id}: dependsOn references unknown node ${dep}`);
        continue;
      }
      if (!seenKnownDeps.has(dep)) {
        knownDeps.push(dep);
        seenKnownDeps.add(dep);
      }
    }
    knownDeps.sort((a, b) => compareNodeOrder(a, b, orderById));
    knownDepsById.set(node.id, knownDeps);

    for (const dep of knownDeps) {
      childrenById.get(dep)?.push(node.id);
    }
  }

  for (const children of childrenById.values()) {
    children.sort((a, b) => compareNodeOrder(a, b, orderById));
  }

  const { topoOrder, cycleIds } = topoSort(nodes, knownDepsById, childrenById, orderById);
  if (cycleIds.length > 0) {
    warnings.push(`cycle or unresolved dependency order detected: ${cycleIds.join(", ")}`);
  }

  return { nodeIds, orderById, depsById, knownDepsById, childrenById, topoOrder, cycleIds };
}

function topoSort(
  nodes: DagNode[],
  knownDepsById: Map<string, string[]>,
  childrenById: Map<string, string[]>,
  orderById: Map<string, number>,
): { topoOrder: string[]; cycleIds: string[] } {
  const indegree = new Map<string, number>();
  for (const node of nodes) {
    indegree.set(node.id, knownDepsById.get(node.id)?.length ?? 0);
  }

  const ready = nodes.filter((node) => (indegree.get(node.id) ?? 0) === 0).map((node) => node.id);
  const topoOrder: string[] = [];

  while (ready.length > 0) {
    ready.sort((a, b) => compareNodeOrder(a, b, orderById));
    const id = ready.shift();
    if (!id) break;
    topoOrder.push(id);

    for (const child of childrenById.get(id) ?? []) {
      const nextIndegree = (indegree.get(child) ?? 0) - 1;
      indegree.set(child, nextIndegree);
      if (nextIndegree === 0) ready.push(child);
    }
  }

  const cycleIds = nodes.map((node) => node.id).filter((id) => !topoOrder.includes(id));
  return { topoOrder, cycleIds };
}

function compareNodeOrder(a: string, b: string, orderById: Map<string, number>): number {
  return (orderById.get(a) ?? Number.MAX_SAFE_INTEGER) - (orderById.get(b) ?? Number.MAX_SAFE_INTEGER) || a.localeCompare(b);
}

function allDependencyIds(depsById: Map<string, string[]>): string[] {
  const ids: string[] = [];
  for (const deps of depsById.values()) ids.push(...deps);
  return ids;
}

function warnForLongIds(ids: string[], width: number, warnings: string[]): void {
  const warned = new Set<string>();
  for (const id of ids) {
    if (id.length > width && !warned.has(id)) {
      warnings.push(`node id exceeds diagram width and was emitted unmodified: ${id}`);
      warned.add(id);
    }
  }
}

function warnForHardEdgeMismatches(dag: DagFile, depsById: Map<string, string[]>, warnings: string[]): void {
  for (const edge of dag.edges ?? []) {
    if (!HARD_EDGE_TYPES.has(edge.type)) continue;
    const targetDeps = depsById.get(edge.to) ?? [];
    if (!targetDeps.includes(edge.from)) {
      const typeLabel = edge.type ?? "hard";
      warnings.push(`hard edge mismatch: ${edge.from} -> ${edge.to} (${typeLabel}) is not represented in ${edge.to}.dependsOn`);
    }
  }
}

function renderDependencyRows(nodes: DagNode[], depsById: Map<string, string[]>, width: number): string[] {
  if (nodes.length === 0) return ["(no nodes)"];

  const idWidth = Math.max(...nodes.map((node) => node.id.length));
  const maxTitleWidth = Math.min(32, Math.max(...nodes.map((node) => node.title.length), 1));

  return nodes.map((node) => {
    const deps = depsById.get(node.id) ?? [];
    const depsText = deps.length > 0 ? deps.join(", ") : "-";
    const prefix = `${node.id.padEnd(idWidth)}  `;
    const suffix = `  deps: ${depsText}`;
    const titleWidth = Math.max(1, Math.min(maxTitleWidth, width - prefix.length - suffix.length));
    const title = fitTitle(node.title, titleWidth);
    return `${prefix}${title.padEnd(titleWidth)}${suffix}`;
  });
}

function fitTitle(title: string, width: number): string {
  if (title.length <= width) return title;
  if (width <= 1) return "…";
  return `${title.slice(0, width - 1)}…`;
}

function renderFirstReady(nodes: DagNode[], depsById: Map<string, string[]>): string {
  const ready = nodes.filter((node) => (depsById.get(node.id) ?? []).length === 0).map((node) => node.id);
  return ready.length > 0 ? ready.join(", ") : "-";
}

function renderSketch(nodes: DagNode[], graph: GraphInfo, width: number): string[] {
  if (nodes.length === 0) return ["(no nodes)"];

  const lines: string[] = [];
  const renderedEdges = new Set<string>();
  const orderedIds = [...graph.topoOrder, ...graph.cycleIds];

  for (const id of orderedIds) {
    const deps = depsForSketch(id, graph);
    if (deps.length > 1) {
      const suffix = collectSingleSuccessorChain(id, graph);
      appendSection(lines, renderFanInBlock(deps, id, suffix, width));
      for (const dep of deps) renderedEdges.add(edgeKey(dep, id));
      markChainEdges([id, ...suffix], renderedEdges);
    } else if (deps.length === 1 && !renderedEdges.has(edgeKey(deps[0], id))) {
      const chain = [deps[0], id, ...collectSingleSuccessorChain(id, graph)];
      appendSection(lines, renderChain(chain, width));
      markChainEdges(chain, renderedEdges);
    }
  }

  for (const node of nodes) {
    const hasDeps = (graph.depsById.get(node.id) ?? []).length > 0;
    const hasChildren = (graph.childrenById.get(node.id) ?? []).length > 0;
    if (!hasDeps && !hasChildren) appendSection(lines, [node.id]);
  }

  return lines.length > 0 ? lines : nodes.map((node) => node.id);
}

function depsForSketch(id: string, graph: GraphInfo): string[] {
  const deps = graph.depsById.get(id) ?? [];
  return [...deps].sort((a, b) => compareNodeOrder(a, b, graph.orderById));
}

function collectSingleSuccessorChain(startId: string, graph: GraphInfo): string[] {
  const suffix: string[] = [];
  const seen = new Set<string>([startId]);
  let current = startId;

  while (true) {
    const children = graph.childrenById.get(current) ?? [];
    if (children.length !== 1) break;

    const next = children[0];
    const nextDeps = graph.knownDepsById.get(next) ?? [];
    if (nextDeps.length !== 1 || seen.has(next)) break;

    suffix.push(next);
    seen.add(next);
    current = next;
  }

  return suffix;
}

function renderFanInBlock(deps: string[], target: string, suffix: string[], width: number): string[] {
  const maxDepWidth = Math.max(...deps.map((dep) => dep.length));
  const connectorIndent = " ".repeat(maxDepWidth + 2);
  const targetPrefix = `${connectorIndent}├─> `;
  const targetChain = [target, ...suffix];
  const includeSuffix = suffix.length > 0 && `${targetPrefix}${targetChain.join(CHAIN_ARROW)}`.length <= width;
  const firstTargetLine = `${targetPrefix}${includeSuffix ? targetChain.join(CHAIN_ARROW) : target}`;
  const lines: string[] = [];

  if (deps.length === 2) {
    lines.push(`${deps[0].padEnd(maxDepWidth)} ─┐`);
    lines.push(firstTargetLine);
    lines.push(`${deps[1].padEnd(maxDepWidth)} ─┘`);
  } else {
    lines.push(`${deps[0].padEnd(maxDepWidth)} ─┐`);
    for (const dep of deps.slice(1, -1)) {
      lines.push(`${dep.padEnd(maxDepWidth)} ─┤`);
    }
    lines.push(firstTargetLine);
    lines.push(`${deps[deps.length - 1].padEnd(maxDepWidth)} ─┘`);
  }

  if (!includeSuffix && suffix.length > 0) {
    lines.push("");
    lines.push(...renderChain(targetChain, width));
  }

  return lines;
}

function renderChain(ids: string[], width: number): string[] {
  if (ids.length === 0) return [];
  if (ids.length === 1) return [ids[0]];

  const lines: string[] = [];
  let current = ids[0];
  let previous = ids[0];

  for (const next of ids.slice(1)) {
    const candidate = `${current}${CHAIN_ARROW}${next}`;
    if (candidate.length > width && current.includes(CHAIN_ARROW)) {
      lines.push(current);
      current = `${previous}${CHAIN_ARROW}${next}`;
    } else {
      current = candidate;
    }
    previous = next;
  }

  lines.push(current);
  return lines;
}

function appendSection(lines: string[], section: string[]): void {
  if (section.length === 0) return;
  if (lines.length > 0 && lines.at(-1) !== "") lines.push("");
  lines.push(...section);
}

function markChainEdges(chain: string[], renderedEdges: Set<string>): void {
  for (let index = 1; index < chain.length; index += 1) {
    renderedEdges.add(edgeKey(chain[index - 1], chain[index]));
  }
}

function edgeKey(from: string, to: string): string {
  return `${from}\u0000${to}`;
}
