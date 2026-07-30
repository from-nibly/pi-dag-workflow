import { mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import { basename, join } from "node:path";

export const BRAINSTORM_TOOLS = [
  "dag_brainstorm_context",
  "dag_brainstorm_update",
  "dag_brainstorm_review",
  "dag_brainstorm_resolve_review",
  "dag_brainstorm_promote",
];

export class FakePi {
  constructor() {
    this.tools = new Map();
    this.commands = new Map();
    this.activeTools = new Set(["read", "bash"]);
    this.beforeAgentStartHandlers = [];
    this.sentUserMessages = [];
  }

  registerTool(tool) { this.tools.set(tool.name, tool); }
  registerCommand(name, command) { this.commands.set(name, command); }
  on(event, handler) { if (event === "before_agent_start") this.beforeAgentStartHandlers.push(handler); }
  getActiveTools() { return [...this.activeTools]; }
  setActiveTools(names) { this.activeTools = new Set(names); }
  sendUserMessage(message) { this.sentUserMessages.push(message); }

  async runCommand(name, args, ctx) {
    const command = this.commands.get(name);
    if (!command) throw new Error(`Unknown command: ${name}`);
    return command.handler(args, ctx);
  }

  async callTool(name, params, ctx) {
    if (!this.activeTools.has(name)) throw new Error(`Inactive tool: ${name}`);
    const tool = this.tools.get(name);
    if (!tool) throw new Error(`Unknown tool: ${name}`);
    return tool.execute(name, params, undefined, undefined, ctx);
  }

  async systemPrompt(base = "BASE") {
    let systemPrompt = base;
    for (const handler of this.beforeAgentStartHandlers) {
      const result = await handler({ systemPrompt });
      if (result?.systemPrompt) systemPrompt = result.systemPrompt;
    }
    return systemPrompt;
  }
}

export class FakeUi {
  constructor(responses = []) { this.responses = [...responses]; this.notifications = []; }
  async select(_title, _choices) { return this.responses.shift(); }
  async input(_title, initial = "") { return this.responses.shift() ?? initial; }
  notify(message, level = "info") { this.notifications.push({ message, level }); }
}

export class BrainstormPiAdapter {
  constructor({ root, domain, renderer, guidance = "STRUCTURED_BRAINSTORM_GUIDANCE" }) {
    this.root = root;
    this.domain = domain;
    this.renderer = renderer;
    this.guidance = guidance;
    this.activeId = undefined;
    this.modeActive = false;
    this.mutationQueues = new Map();
  }

  register(pi) {
    for (const name of BRAINSTORM_TOOLS) {
      pi.registerTool({
        name,
        label: name,
        description: `Structured brainstorm adapter: ${name}`,
        execute: async (_callId, params, _signal, _update, ctx) => this.executeTool(name, params, ctx),
      });
    }
    pi.setActiveTools(pi.getActiveTools().filter((name) => !BRAINSTORM_TOOLS.includes(name)));

    pi.registerCommand("dag", {
      handler: async (args, ctx) => {
        const parsed = parseDagArgs(args);
        if (parsed.command === "brainstorm") return this.startBrainstorm(pi, parsed, ctx);
        this.suspend(pi);
        pi.sendUserMessage(`Run /dag ${parsed.command || "help"}`);
      },
    });

    pi.on("before_agent_start", async (event) => {
      if (!this.modeActive) return;
      return { systemPrompt: `${event.systemPrompt}\n\n${this.guidance}\nActive brainstorm: ${this.activeId}` };
    });
  }

  async startBrainstorm(pi, parsed, ctx) {
    const candidates = await listSnapshots(this.root);
    let selected;

    if (!ctx.hasUI) {
      if (parsed.continueId) selected = requireCandidate(candidates, parsed.continueId);
      else if (parsed.newId) selected = await createSnapshot(this.root, parsed.newId, parsed.seed || parsed.newId);
      else throw new Error(headlessSelectionError(candidates));
    } else if (parsed.continueId) {
      selected = requireCandidate(candidates, parsed.continueId);
    } else if (parsed.newId) {
      selected = await createSnapshot(this.root, parsed.newId, parsed.seed || parsed.newId);
    } else if (candidates.length) {
      const action = await ctx.ui.select("Structured brainstorm", ["Resume", "New"]);
      if (action === "Resume") {
        const id = candidates.length === 1
          ? candidates[0].id
          : await ctx.ui.select("Resume which brainstorm?", candidates.map(({ id }) => id));
        selected = requireCandidate(candidates, id);
      } else if (action === "New") {
        const id = await ctx.ui.input("Brainstorm id");
        selected = await createSnapshot(this.root, id, parsed.seed || id);
      } else {
        return;
      }
    } else {
      const id = parsed.seed ? slug(parsed.seed) : await ctx.ui.input("Brainstorm id");
      selected = await createSnapshot(this.root, id, parsed.seed || id);
    }

    this.activeId = selected.id;
    this.modeActive = true;
    pi.setActiveTools([...new Set([...pi.getActiveTools(), ...BRAINSTORM_TOOLS])]);
    const orientation = await this.domain.context(selected.path, { id: selected.id, view: "orientation" });
    pi.sendUserMessage(`Resume structured brainstorm ${selected.id}.\nOrientation: ${JSON.stringify(orientation)}\nUser seed: ${parsed.seed || "(none)"}`);
    return selected;
  }

  suspend(pi) {
    this.modeActive = false;
    this.activeId = undefined;
    pi.setActiveTools(pi.getActiveTools().filter((name) => !BRAINSTORM_TOOLS.includes(name)));
  }

  async executeTool(name, params, ctx) {
    if (!this.modeActive || !this.activeId) throw new Error("No active structured brainstorm. Run /dag brainstorm first.");
    if (params.id && params.id !== this.activeId) throw new Error(`Active brainstorm is ${this.activeId}, not ${params.id}`);
    const path = snapshotPath(this.root, this.activeId);
    const invoke = async () => {
      if (name === "dag_brainstorm_context") return compactResult(await this.domain.context(path, { ...params, id: this.activeId }));
      if (name === "dag_brainstorm_update") return compactResult(await this.domain.update(path, { ...params, id: this.activeId }));
      if (name === "dag_brainstorm_review") {
        const result = await this.domain.review(path, { ...params, id: this.activeId });
        if ((params.presentation ?? "chat") === "lavish") {
          try {
            const submission = await this.renderer.present(result.review);
            return compactResult(result, { submission, presentation: "lavish" });
          } catch (error) {
            return compactResult(result, { presentation: "lavish", rendererError: error.message });
          }
        }
        return compactResult(result, { presentation: "chat", review: result.review });
      }
      if (name === "dag_brainstorm_resolve_review") return compactResult(await this.domain.resolveReview(path, { ...params, id: this.activeId }));
      if (name === "dag_brainstorm_promote") {
        const action = params.action ?? "prepare";
        const result = await this.domain.promote(path, { ...params, id: this.activeId, cwd: ctx.cwd });
        return compactResult(result, { action });
      }
      throw new Error(`Unknown brainstorm tool: ${name}`);
    };

    return isMutating(name, params) ? this.withMutationQueue(path, invoke) : invoke();
  }

  async withMutationQueue(path, fn) {
    const previous = this.mutationQueues.get(path) ?? Promise.resolve();
    const current = previous.catch(() => {}).then(fn);
    this.mutationQueues.set(path, current);
    try { return await current; }
    finally { if (this.mutationQueues.get(path) === current) this.mutationQueues.delete(path); }
  }
}

export async function listSnapshots(root) {
  const dir = join(root, ".ai", "brainstorm");
  let names;
  try { names = await readdir(dir); } catch { return []; }
  const snapshots = [];
  for (const name of names.filter((item) => item.endsWith(".json")).sort()) {
    try {
      const path = join(dir, name);
      const state = JSON.parse(await readFile(path, "utf8"));
      if (!state.archivedAt) snapshots.push({ id: state.id ?? basename(name, ".json"), title: state.title, revision: state.revision ?? 0, path });
    } catch {
      // Invalid disposable snapshots are omitted from interactive resume choices.
    }
  }
  return snapshots;
}

export async function createSnapshot(root, id, title) {
  if (!id?.trim()) throw new Error("New brainstorm requires an id");
  const path = snapshotPath(root, id);
  await mkdir(join(root, ".ai", "brainstorm"), { recursive: true });
  const state = { schemaVersion: 2, id, title: title || id, revision: 0, currentUnderstanding: null, reviews: [] };
  await writeFile(path, `${JSON.stringify(state, null, 2)}\n`, "utf8");
  return { id, title: state.title, revision: 0, path };
}

export function snapshotPath(root, id) { return join(root, ".ai", "brainstorm", `${id}.json`); }

function compactResult(result, extra = {}) {
  const summary = result.summary ?? result.action ?? "ok";
  return {
    content: [{ type: "text", text: String(summary) }],
    details: { revision: result.revision, changed: result.changed ?? {}, ...extra },
  };
}

function isMutating(name, params) {
  if (["dag_brainstorm_update", "dag_brainstorm_review", "dag_brainstorm_resolve_review"].includes(name)) return true;
  return name === "dag_brainstorm_promote" && (params.action ?? "prepare") === "record";
}

function parseDagArgs(input) {
  const tokens = String(input).trim().split(/\s+/).filter(Boolean);
  const command = tokens.shift() ?? "";
  const result = { command, seed: [] };
  for (let i = 0; i < tokens.length; i++) {
    if (tokens[i] === "--continue") result.continueId = tokens[++i];
    else if (tokens[i] === "--new") result.newId = tokens[++i];
    else result.seed.push(tokens[i]);
  }
  result.seed = result.seed.join(" ");
  return result;
}

function requireCandidate(candidates, id) {
  const candidate = candidates.find((item) => item.id === id);
  if (!candidate) throw new Error(`Brainstorm snapshot not found: ${id}`);
  return candidate;
}

function headlessSelectionError(candidates) {
  const available = candidates.length ? ` Available: ${candidates.map(({ id }) => id).join(", ")}.` : "";
  return `Headless /dag brainstorm requires --continue <id> or --new <id>.${available}`;
}

function slug(value) { return String(value).trim().toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 64) || "brainstorm"; }
