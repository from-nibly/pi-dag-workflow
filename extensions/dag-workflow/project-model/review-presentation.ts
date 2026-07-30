import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { LavishCliAdapter, type LavishFeedback } from "./lavish-cli.ts";
import { renderReviewTurn } from "./review-renderer.ts";
import { assertTurnProjection, reviewArtifactPaths, type ModelReviewTurnProjection } from "./review-turn.ts";

export type PresentationStatus = "rendered" | "open" | "feedback" | "interrupted" | "user_ended" | "ended";
export interface ReviewPresentationMetadata {
  schemaVersion: 1;
  focusId: string;
  reviewId: string;
  artifactPath: string;
  reviewHash: string;
  status: PresentationStatus;
  userEnded: boolean;
  updatedAt: string;
  openedStatus?: string;
}
export interface PresentationUpdate { phase: PresentationStatus | "waiting"; artifactPath?: string; promptCount?: number; warningCount?: number; resumed?: boolean }

export class ReviewPresentationManager {
  readonly root: string;
  readonly cli: LavishCliAdapter;
  readonly clock: () => string;

  constructor(root: string, input: { cli?: LavishCliAdapter; clock?: () => string } = {}) {
    this.root = root;
    this.cli = input.cli ?? new LavishCliAdapter({ cwd: root });
    this.clock = input.clock ?? (() => new Date().toISOString());
  }

  paths(projection: ModelReviewTurnProjection) { return reviewArtifactPaths(this.root, projection.focus.id, projection.review.id); }

  async render(projection: ModelReviewTurnProjection) {
    assertTurnProjection(projection);
    const paths = this.paths(projection);
    const html = renderReviewTurn(projection);
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

  async present(projection: ModelReviewTurnProjection, input: { signal?: AbortSignal; onUpdate?: (event: PresentationUpdate) => void; onPresented?: () => Promise<void>; noOpen?: boolean } = {}) {
    assertTurnProjection(projection);
    const existing = await this.readMetadata(this.paths(projection));
    if (existing?.userEnded) throw new Error("Lavish session was ended by the user; use resume with explicit reopen");
    const rendered = await this.render(projection);
    input.onUpdate?.({ phase: "rendered", artifactPath: rendered.paths.html });
    const opened = await this.cli.open(rendered.paths.html, { signal: input.signal, noOpen: input.noOpen });
    const metadata = await this.writeMetadata(rendered.paths, { ...rendered.metadata, status: "open", openedStatus: opened.status, updatedAt: this.clock() });
    await input.onPresented?.();
    input.onUpdate?.({ phase: "waiting", artifactPath: rendered.paths.html });
    return this.poll(rendered.paths, metadata, input);
  }

  async resume(projection: ModelReviewTurnProjection, input: { signal?: AbortSignal; onUpdate?: (event: PresentationUpdate) => void; onPresented?: () => Promise<void>; reopen?: boolean } = {}) {
    assertTurnProjection(projection);
    const paths = this.paths(projection);
    let metadata = await this.readMetadata(paths);
    const missing = !metadata;
    const changed = metadata?.reviewHash !== projection.review.semanticHash;
    if (missing) metadata = (await this.render(projection)).metadata;
    else if (changed) {
      const previous = metadata!;
      metadata = (await this.render(projection)).metadata;
      if (previous.userEnded) metadata = await this.writeMetadata(paths, { ...metadata, status: "user_ended", userEnded: true, updatedAt: this.clock() });
    }
    if (!metadata) throw new Error("Presentation metadata could not be initialized");
    if (metadata.userEnded && !input.reopen) throw new Error("Lavish session was ended by the user; explicit reopen is required");
    if (missing || metadata.status === "rendered") {
      const opened = await this.cli.open(paths.html, { signal: input.signal });
      metadata = await this.writeMetadata(paths, { ...metadata, status: "open", openedStatus: opened.status, updatedAt: this.clock() });
    } else if (input.reopen) {
      const opened = await this.cli.open(paths.html, { signal: input.signal, reopen: true });
      metadata = await this.writeMetadata(paths, { ...metadata, status: "open", openedStatus: opened.status, userEnded: false, updatedAt: this.clock() });
    }
    await input.onPresented?.();
    input.onUpdate?.({ phase: "waiting", artifactPath: paths.html, resumed: true });
    return this.poll(paths, metadata, input);
  }

  async poll(paths: ReturnType<typeof reviewArtifactPaths>, metadata: ReviewPresentationMetadata, input: { signal?: AbortSignal; onUpdate?: (event: PresentationUpdate) => void } = {}): Promise<{ paths: typeof paths; metadata: ReviewPresentationMetadata; feedback: LavishFeedback }> {
    try {
      const feedback = await this.cli.poll(paths.html, { signal: input.signal });
      const userEnded = feedback.session.endedBy === "user" || Boolean(feedback.session.sessionEnded && feedback.session.endedBy === "user");
      const status: PresentationStatus = userEnded ? "user_ended" : feedback.session.status === "feedback" ? "feedback" : feedback.session.status === "ended" ? "ended" : "open";
      const updated = await this.writeMetadata(paths, { ...metadata, status, userEnded, updatedAt: this.clock() });
      input.onUpdate?.({ phase: status, promptCount: feedback.prompts.length, warningCount: feedback.layoutWarnings.length });
      return { paths, metadata: updated, feedback };
    } catch (error: any) {
      if (error?.name === "AbortError" || input.signal?.aborted) await this.writeMetadata(paths, { ...metadata, status: "interrupted", updatedAt: this.clock() });
      throw error;
    }
  }

  async end(projection: ModelReviewTurnProjection, signal?: AbortSignal) {
    const paths = this.paths(projection);
    let result;
    try { result = await this.cli.end(paths.html, { signal }); }
    finally {
      const previous = await this.readMetadata(paths);
      if (previous) await this.writeMetadata(paths, { ...previous, status: "ended", updatedAt: this.clock() });
    }
    return { paths, result };
  }

  async cleanup(focusId: string, reviewId: string) {
    const paths = reviewArtifactPaths(this.root, focusId, reviewId);
    try { await this.cli.end(paths.html); } catch {}
    await Promise.all([rm(paths.html, { force: true }), rm(paths.metadata, { force: true })]);
    return { paths, removed: true };
  }

  async readMetadata(paths: ReturnType<typeof reviewArtifactPaths>): Promise<ReviewPresentationMetadata | undefined> {
    try { return JSON.parse(await readFile(paths.metadata, "utf8")) as ReviewPresentationMetadata; }
    catch (error: any) { if (error?.code === "ENOENT") return undefined; throw error; }
  }

  async writeMetadata(paths: ReturnType<typeof reviewArtifactPaths>, metadata: ReviewPresentationMetadata): Promise<ReviewPresentationMetadata> {
    await atomicWrite(paths.metadata, `${JSON.stringify(metadata, null, 2)}\n`);
    return metadata;
  }
}

async function atomicWrite(path: string, content: string) {
  await mkdir(dirname(path), { recursive: true });
  const temporary = `${path}.${process.pid}.${Date.now()}.tmp`;
  await writeFile(temporary, content, "utf8");
  await rename(temporary, path);
}
