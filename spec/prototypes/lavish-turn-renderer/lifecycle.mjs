import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { artifactPaths, assertTurnProjection } from "./projection.mjs";
import { renderTurn } from "./renderer.mjs";

export class LavishTurnLifecycle {
  constructor({ root, cli, clock = () => new Date().toISOString() }) {
    this.root = root;
    this.cli = cli;
    this.clock = clock;
  }

  paths(projection) { return artifactPaths(this.root, projection.focus.id, projection.review.id); }

  async render(projection) {
    assertTurnProjection(projection);
    const paths = this.paths(projection);
    const html = renderTurn(projection);
    await atomicWrite(paths.html, html);
    const metadata = await this.writeMetadata(paths, {
      schemaVersion: 1,
      focusId: projection.focus.id,
      reviewId: projection.review.id,
      artifactPath: paths.html,
      reviewHash: projection.review.semanticHash,
      status: "rendered",
      userEnded: false,
      updatedAt: this.clock(),
    });
    return { paths, html, metadata };
  }

  async present(projection, { signal, onUpdate, noOpen = false } = {}) {
    const rendered = await this.render(projection);
    onUpdate?.({ phase: "rendered", artifactPath: rendered.paths.html });
    const opened = await this.cli.open(rendered.paths.html, { signal, noOpen });
    await this.writeMetadata(rendered.paths, { ...rendered.metadata, status: "open", openedStatus: opened.status, updatedAt: this.clock() });
    onUpdate?.({ phase: "waiting", artifactPath: rendered.paths.html });
    return this.poll(rendered.paths, { signal, onUpdate });
  }

  async resume(projection, { signal, onUpdate, reopen = false } = {}) {
    assertTurnProjection(projection);
    const paths = this.paths(projection);
    let metadata = await this.readMetadata(paths);
    const missing = !metadata;
    const changed = metadata?.reviewHash !== projection.review.semanticHash;
    if (missing) metadata = (await this.render(projection)).metadata;
    else if (changed) {
      const previous = metadata;
      metadata = (await this.render(projection)).metadata;
      if (previous.userEnded) metadata = await this.writeMetadata(paths, { ...metadata, status: "user_ended", userEnded: true, updatedAt: this.clock() });
    }
    if (metadata?.userEnded && !reopen) throw new Error("Lavish session was ended by the user; explicit reopen is required");
    if (missing || metadata.status === "rendered") await this.cli.open(paths.html, { signal });
    else if (reopen) await this.cli.open(paths.html, { signal, reopen: true });
    onUpdate?.({ phase: "waiting", artifactPath: paths.html, resumed: true });
    return this.poll(paths, { signal, onUpdate });
  }

  async poll(paths, { signal, onUpdate } = {}) {
    try {
      const feedback = await this.cli.poll(paths.html, { signal });
      const userEnded = feedback.session.endedBy === "user" || Boolean(feedback.session.sessionEnded && feedback.session.endedBy === "user");
      const status = userEnded ? "user_ended" : feedback.session.status === "feedback" ? "feedback" : feedback.session.status === "ended" ? "ended" : "open";
      const previous = await this.readMetadata(paths);
      const metadata = await this.writeMetadata(paths, { ...previous, status, userEnded, updatedAt: this.clock() });
      onUpdate?.({ phase: status, promptCount: feedback.prompts.length, warningCount: feedback.layoutWarnings.length });
      return { paths, metadata, feedback };
    } catch (error) {
      if (error?.name === "AbortError" || signal?.aborted) {
        const previous = await this.readMetadata(paths);
        await this.writeMetadata(paths, { ...previous, status: "interrupted", updatedAt: this.clock() });
      }
      throw error;
    }
  }

  async end(projection) {
    const paths = this.paths(projection);
    let result;
    try { result = await this.cli.end(paths.html); }
    finally {
      const previous = await this.readMetadata(paths);
      await this.writeMetadata(paths, { ...previous, status: "ended", userEnded: previous?.userEnded ?? false, updatedAt: this.clock() });
    }
    return { paths, result };
  }

  async cleanup(projection) {
    const paths = this.paths(projection);
    try { await this.cli.end(paths.html); } catch {}
    await Promise.all([rm(paths.html, { force: true }), rm(paths.metadata, { force: true })]);
    return { paths, removed: true };
  }

  async readMetadata(paths) {
    try { return JSON.parse(await readFile(paths.metadata, "utf8")); }
    catch (error) { if (error?.code === "ENOENT") return undefined; throw error; }
  }

  async writeMetadata(paths, metadata) {
    await atomicWrite(paths.metadata, `${JSON.stringify(metadata, null, 2)}\n`);
    return metadata;
  }
}

async function atomicWrite(path, content) {
  await mkdir(dirname(path), { recursive: true });
  const temporary = `${path}.${process.pid}.${Date.now()}.tmp`;
  await writeFile(temporary, content, "utf8");
  await rename(temporary, path);
}
