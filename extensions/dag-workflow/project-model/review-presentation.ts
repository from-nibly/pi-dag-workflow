import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { LavishCliAdapter, type LavishFeedback } from "./lavish-cli.ts";
import { withFileLock } from "./persistence.ts";
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
  feedbackDrainedAt?: string;
}
export interface PresentationUpdate { phase: PresentationStatus | "waiting"; artifactPath?: string; promptCount?: number; warningCount?: number; resumed?: boolean }

export class ReviewPresentationManager {
  readonly root: string;
  readonly cli: LavishCliAdapter;
  readonly clock: () => string;
  #activeArtifacts = new Set<string>();

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
    const paths = this.paths(projection);
    return this.#exclusive(paths, async () => {
      const existing = await this.readMetadata(paths);
      if (existing?.userEnded) throw new Error("Lavish session was ended by the user; use resume with explicit reopen");
      if (existing?.reviewHash === projection.review.semanticHash) throw new Error("This exact Lavish review is already presented; use collect for submitted feedback or resume only to continue waiting");
      if (existing && (existing.status !== "ended" || !existing.feedbackDrainedAt)) throw new Error("The prior Lavish review may still contain uncollected feedback; successfully end and drain it before presenting a changed review");
      const rendered = await this.render(projection);
      input.onUpdate?.({ phase: "rendered", artifactPath: rendered.paths.html });
      const opened = await this.cli.open(rendered.paths.html, { signal: input.signal, noOpen: input.noOpen });
      const metadata = await this.writeMetadata(rendered.paths, { ...rendered.metadata, status: "open", openedStatus: opened.status, updatedAt: this.clock() });
      await input.onPresented?.();
      input.onUpdate?.({ phase: "waiting", artifactPath: rendered.paths.html });
      return this.poll(rendered.paths, metadata, input);
    });
  }

  async resume(projection: ModelReviewTurnProjection, input: { signal?: AbortSignal; onUpdate?: (event: PresentationUpdate) => void; onPresented?: () => Promise<void>; reopen?: boolean } = {}) {
    assertTurnProjection(projection);
    const paths = this.paths(projection);
    return this.#exclusive(paths, async () => {
      let metadata = await this.readMetadata(paths);
      const missing = !metadata;
      if (metadata && metadata.reviewHash !== projection.review.semanticHash) throw new Error("The Lavish artifact belongs to a different review revision; collect and end it before presenting the changed review");
      if (missing) metadata = (await this.render(projection)).metadata;
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
    });
  }

  async collect(projection: ModelReviewTurnProjection, input: { signal?: AbortSignal; onUpdate?: (event: PresentationUpdate) => void } = {}) {
    assertTurnProjection(projection);
    const paths = this.paths(projection);
    return this.#exclusive(paths, async () => {
      const metadata = await this.readMetadata(paths);
      if (!metadata) throw new Error("Lavish review has not been presented; use present first");
      return this.poll(paths, metadata, { ...input, collectOnly: true });
    });
  }

  async poll(paths: ReturnType<typeof reviewArtifactPaths>, metadata: ReviewPresentationMetadata, input: { signal?: AbortSignal; onUpdate?: (event: PresentationUpdate) => void; collectOnly?: boolean } = {}): Promise<{ paths: typeof paths; metadata: ReviewPresentationMetadata; feedback: LavishFeedback }> {
    try {
      const feedback = input.collectOnly ? await this.cli.collect(paths.html, { signal: input.signal }) : await this.cli.poll(paths.html, { signal: input.signal });
      const userEnded = feedback.session.endedBy === "user" || Boolean(feedback.session.sessionEnded && feedback.session.endedBy === "user");
      const status: PresentationStatus = userEnded ? "user_ended" : feedback.session.status === "feedback" ? "feedback" : feedback.session.status === "ended" ? "ended" : "open";
      const feedbackIsFinal = feedback.session.status === "ended" || Boolean(feedback.session.sessionEnded);
      const updated = await this.writeMetadata(paths, { ...metadata, status, userEnded, updatedAt: this.clock(), ...(feedbackIsFinal ? { feedbackDrainedAt: this.clock() } : {}) });
      input.onUpdate?.({ phase: status, promptCount: feedback.prompts.length, warningCount: feedback.layoutWarnings.length });
      return { paths, metadata: updated, feedback };
    } catch (error: any) {
      if (error?.name === "AbortError" || input.signal?.aborted) await this.writeMetadata(paths, { ...metadata, status: "interrupted", updatedAt: this.clock() });
      throw error;
    }
  }

  async end(projection: ModelReviewTurnProjection, signal?: AbortSignal) {
    const paths = this.paths(projection);
    return this.#exclusive(paths, async () => {
      const previous = await this.readMetadata(paths);
      if (!previous) throw new Error("Lavish review has not been presented");
      let result;
      try { result = await this.cli.end(paths.html, { signal }); }
      catch (error) {
        await this.writeMetadata(paths, { ...previous, status: "interrupted", updatedAt: this.clock() });
        throw error;
      }
      const ending = await this.writeMetadata(paths, { ...previous, status: "ending", updatedAt: this.clock() });
      let feedback;
      try { feedback = await this.cli.collect(paths.html, { signal }); }
      catch (error) {
        await this.writeMetadata(paths, { ...ending, status: "interrupted", updatedAt: this.clock() });
        throw error;
      }
      const metadata = await this.writeMetadata(paths, { ...ending, status: "ended", feedbackDrainedAt: this.clock(), updatedAt: this.clock() });
      return { paths, result, metadata, feedback };
    });
  }

  async cleanup(focusId: string, reviewId: string) {
    const paths = reviewArtifactPaths(this.root, focusId, reviewId);
    return this.#exclusive(paths, async () => {
      const metadata = await this.readMetadata(paths);
      if (metadata && (!metadata.feedbackDrainedAt || !["ended", "user_ended"].includes(metadata.status))) throw new Error("Cannot remove a Lavish artifact before its exact session is ended and final feedback is drained");
      if (!metadata) return { paths, removed: false };
      await Promise.all([rm(paths.html, { force: true }), rm(paths.metadata, { force: true })]);
      return { paths, removed: true };
    });
  }

  async readMetadata(paths: ReturnType<typeof reviewArtifactPaths>): Promise<ReviewPresentationMetadata | undefined> {
    try { return JSON.parse(await readFile(paths.metadata, "utf8")) as ReviewPresentationMetadata; }
    catch (error: any) { if (error?.code === "ENOENT") return undefined; throw error; }
  }

  async writeMetadata(paths: ReturnType<typeof reviewArtifactPaths>, metadata: ReviewPresentationMetadata): Promise<ReviewPresentationMetadata> {
    await atomicWrite(paths.metadata, `${JSON.stringify(metadata, null, 2)}\n`);
    return metadata;
  }

  async #exclusive<T>(paths: ReturnType<typeof reviewArtifactPaths>, operation: () => Promise<T>): Promise<T> {
    const release = this.#claim(paths.html);
    try { return await withFileLock(paths.metadata, operation); }
    finally { release(); }
  }

  #claim(artifactPath: string): () => void {
    if (this.#activeArtifacts.has(artifactPath)) throw new Error("Another Lavish presentation or feedback operation is already active for this exact review");
    this.#activeArtifacts.add(artifactPath);
    return () => { this.#activeArtifacts.delete(artifactPath); };
  }
}

async function atomicWrite(path: string, content: string) {
  await mkdir(dirname(path), { recursive: true });
  const temporary = `${path}.${process.pid}.${Date.now()}.tmp`;
  await writeFile(temporary, content, "utf8");
  await rename(temporary, path);
}
