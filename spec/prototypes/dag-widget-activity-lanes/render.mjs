const SPINNER = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
const STAGES = ["F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8"];

export const WIDTHS = [50, 80, 120];
export const VARIANTS = ["flow", "graph", "rail", "cards"];
export const SCENARIOS = {
  parallel: {
    label: "Two active lanes",
    run: "run-64e81c4f · r21 · active 2/7 · ready 0 · attention 0",
    nodes: [
      { alias: "N01", glyph: "*", stage: "F3", passed: 3, title: "Verify domain changes", live: true, dependents: ["N06"] },
      { alias: "N02", glyph: ":", stage: "F1", passed: 1, title: "Migrate authority model", live: false, dependents: ["N03", "N04", "N05"] },
    ],
    omitted: "+2 nodes outside activity neighborhood",
  },
  fanout: {
    label: "Wide fan-out",
    run: "run-fanout · r38 · active 1/12 · ready 4 · attention 0",
    nodes: [
      { alias: "N02", glyph: "*", stage: "F5", passed: 5, title: "Migrate authorization boundary", live: true, dependents: ["N03", "N04", "N05", "N08", "N09"] },
    ],
    omitted: "+6 nodes · +2 downstream edges omitted",
  },
  attention: {
    label: "Attention plus live work",
    run: "run-attention · r52 · active 2/9 · ready 1 · attention 1",
    nodes: [
      { alias: "N05", glyph: "!", stage: "F6", passed: 5, title: "Prove compatibility contract", live: false, dependents: ["N07"], detail: "retry exhausted · conductor direction needed" },
      { alias: "N03", glyph: "*", stage: "F2", passed: 2, title: "Validate runtime rename", live: true, dependents: ["N06"] },
    ],
    omitted: "+4 pending · 1 integration head",
  },
  stale: {
    label: "Stale liveness",
    run: "run-stale · r17 · STALE READ-ONLY · active 1/6",
    nodes: [
      { alias: "N04", glyph: "*", stage: "F4", passed: 4, title: "Run deterministic checks", live: false, frozen: true, dependents: ["N06"] },
    ],
    omitted: "worker liveness last observed 8s ago · motion frozen",
  },
};

function widthOf(value) {
  return Array.from(value).length;
}

function fit(value, width) {
  if (width <= 0) return "";
  const chars = Array.from(value);
  if (chars.length <= width) return value;
  if (width === 1) return "…";
  return `${chars.slice(0, width - 1).join("")}…`;
}

function pad(value, width) {
  const clipped = fit(value, width);
  return clipped + " ".repeat(Math.max(0, width - widthOf(clipped)));
}

function activity(node, frame) {
  if (node.live) return SPINNER[frame % SPINNER.length];
  if (node.frozen) return "·";
  return " ";
}

function progress(node, width) {
  if (width < 60) return `${node.stage} ${node.passed}/9`;
  const current = STAGES.indexOf(node.stage);
  const cells = STAGES.map((_, index) => index < node.passed ? "■" : index === current ? "▶" : "·").join("");
  return `${node.stage} [${cells}]`;
}

function dependencies(node, available) {
  if (available < 5 || node.dependents.length === 0) return "";
  const full = ` → ${node.dependents.join(" ")}`;
  if (widthOf(full) <= available) return full;
  for (let count = node.dependents.length - 1; count >= 1; count -= 1) {
    const value = ` → ${node.dependents.slice(0, count).join(" ")} +${node.dependents.length - count}`;
    if (widthOf(value) <= available) return value;
  }
  return fit(` → +${node.dependents.length}`, available);
}

function flowRows(scenario, width, frame) {
  const rows = [];
  for (const node of scenario.nodes) {
    const semantic = `${activity(node, frame)} ${node.glyph}${node.alias} ${progress(node, width)}`;
    const remaining = Math.max(0, width - widthOf(semantic) - 1);
    const dependencyReserve = Math.min(remaining, Math.max(8, node.dependents.length * 5 + 3));
    const titleWidth = Math.max(0, remaining - dependencyReserve);
    const title = titleWidth ? ` ${fit(node.title, titleWidth)}` : "";
    const deps = dependencies(node, Math.max(0, width - widthOf(semantic + title)));
    rows.push(fit(`${semantic}${title}${deps}`, width));
    if (node.detail && rows.length < 9) rows.push(fit(`  └ ! ${node.detail}`, width));
  }
  return rows;
}

function graphRows(scenario, width, frame) {
  const rows = [];
  for (const node of scenario.nodes) {
    const rawSemantic = `${activity(node, frame)} ${node.glyph}${node.alias} ${progress(node, width)} ${node.title}${node.detail ? ` ! ${node.detail}` : ""}`;
    rows.push(fit(rawSemantic, width));
    const sourceColumn = 3;
    const lastColumn = width - 6;
    const desiredFirstColumn = Math.max(4, Math.floor(width * 0.38));
    const packedFirstColumn = lastColumn - (node.dependents.length - 1) * 7;
    const firstColumn = Math.max(4, Math.min(desiredFirstColumn, packedFirstColumn));
    const positions = node.dependents.map((_, index) => node.dependents.length === 1 ? lastColumn : Math.round(firstColumn + (lastColumn - firstColumn) * index / (node.dependents.length - 1)));
    const connector = Array(width).fill(" ");
    connector[sourceColumn] = "└";
    for (let column = sourceColumn + 1; column <= lastColumn; column += 1) connector[column] = "─";
    positions.forEach((column, index) => { connector[column] = index === positions.length - 1 ? "┐" : "┬"; });
    const dependents = Array(width).fill(" ");
    positions.forEach((column, index) => {
      Array.from(`▶ ${node.dependents[index]}`).forEach((character, offset) => { if (column + offset < width) dependents[column + offset] = character; });
    });
    rows.push(connector.join("").trimEnd());
    rows.push(dependents.join("").trimEnd());
  }
  return rows;
}

function railRows(scenario, width, frame) {
  const rows = [];
  for (const node of scenario.nodes) {
    const topology = `${node.glyph}${node.alias} ${fit(node.title, Math.max(8, Math.floor(width * 0.45)))}${dependencies(node, Math.floor(width * 0.45))}`;
    rows.push(fit(topology, width));
    rows.push(fit(`  ${activity(node, frame)} ${progress(node, width)}${node.detail ? ` · ${node.detail}` : ""}`, width));
  }
  return rows;
}

function cardRows(scenario, width, frame) {
  const gap = 2;
  const count = width >= 96 ? Math.min(2, scenario.nodes.length) : 1;
  const cardWidth = Math.max(18, Math.floor((width - gap * (count - 1)) / count));
  const chunks = [];
  for (let offset = 0; offset < scenario.nodes.length; offset += count) {
    const group = scenario.nodes.slice(offset, offset + count);
    const cards = group.map((node) => [
      `┌${"─".repeat(cardWidth - 2)}┐`,
      `│${pad(`${activity(node, frame)} ${node.glyph}${node.alias} ${progress(node, cardWidth)}`, cardWidth - 2)}│`,
      `│${pad(node.title, cardWidth - 2)}│`,
      `│${pad(node.dependents.length ? `→ ${node.dependents.join(" ")}` : "→ none", cardWidth - 2)}│`,
      `└${"─".repeat(cardWidth - 2)}┘`,
    ]);
    for (let row = 0; row < 5; row += 1) chunks.push(fit(cards.map((card) => card[row]).join(" ".repeat(gap)), width));
  }
  return chunks;
}

export function renderPrototype({ variant = "flow", scenario = "parallel", width = 80, frame = 0 } = {}) {
  if (!VARIANTS.includes(variant)) throw new Error(`Unknown variant: ${variant}`);
  if (!SCENARIOS[scenario]) throw new Error(`Unknown scenario: ${scenario}`);
  if (!Number.isInteger(width) || width < 20) throw new Error("Prototype width must be an integer >= 20");
  const fixture = SCENARIOS[scenario];
  const rowBudget = Math.max(4, Math.min(12, Math.floor(36 / 3)));
  const header = fit(`DAG ${fixture.run}`, width);
  const content = variant === "flow" ? flowRows(fixture, width, frame) : variant === "graph" ? graphRows(fixture, width, frame) : variant === "rail" ? railRows(fixture, width, frame) : cardRows(fixture, width, frame);
  const footer = fit(fixture.omitted, width);
  const available = rowBudget - 2;
  const visible = content.slice(0, available);
  if (content.length > available) visible[available - 1] = fit(`… ${content.length - available + 1} prototype rows omitted`, width);
  return [header, ...visible, footer];
}

export function assertPrototypeLayout(lines, width) {
  if (lines.length > 12) throw new Error(`row budget exceeded: ${lines.length}`);
  for (const [index, line] of lines.entries()) {
    if (widthOf(line) > width) throw new Error(`line ${index + 1} exceeds ${width}: ${widthOf(line)}`);
  }
}
