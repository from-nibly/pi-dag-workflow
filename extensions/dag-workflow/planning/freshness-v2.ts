import { readFile, lstat, realpath } from "node:fs/promises";
import { resolve, relative, sep } from "node:path";
import { createHash } from "node:crypto";
import { canonicalHash } from "../dag-runtime/common.ts";
import { allObjects, assertValidProjectModel, semanticHash, specEligibleObjectIds } from "../project-model/model.ts";
import { SpecProjector } from "../project-model/projector.ts";
import { bindGitV2, nativeGitV2, assertGitAttributesV2, assertTargetV2 } from "../runtime-v2/git-native.ts";
import { requireV2, sameV2, validateShapeV2, SourceScopeV2Schema, type PlanV2 } from "./v2.ts";
import type { AcceptanceV2 } from "../runtime-v2/state.ts";
import type { FreshnessV2 } from "../runtime-v2/service.ts";

const digest = (bytes: Buffer | string) => `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
/** Resolve only tracked, regular, in-root HEAD bytes. No caller-provided digest is used. */
async function tracked(root: string, head: string, path: string): Promise<string> {
  requireV2(path.length > 0 && !path.includes("\0") && !path.includes("\\") && !path.startsWith("/") && !path.split("/").some(p => !p || p === "." || p === ".."), "UNSAFE_SOURCE_PATH");
  const absolute = resolve(root, path);
  requireV2(!relative(root, absolute).startsWith(`..${sep}`), "SOURCE_OUTSIDE_ROOT");
  let current = root;
  for (const part of path.split("/")) {
    current = resolve(current, part);
    const stat = await lstat(current).catch((error: NodeJS.ErrnoException) => {
      if (error.code === "ENOENT") throw Error(`SOURCE_NOT_FOUND: ${path}`);
      throw error;
    });
    requireV2(!stat.isSymbolicLink(), "SYMLINK_SOURCE_UNSUPPORTED");
  }
  requireV2((await lstat(absolute)).isFile() && await realpath(absolute) === absolute, "REGULAR_SOURCE_REQUIRED");
  const entry = nativeGitV2(root, "ls-tree", head, "--", path);
  requireV2(/^100(?:644|755) blob [0-9a-f]+\t/.test(entry), "TRACKED_REGULAR_SOURCE_REQUIRED");
  const bytes = await readFile(absolute);
  // cat-file's text output must be lossless (nativeGitV2 trims terminal whitespace).
  const text = bytes.toString("utf8"); requireV2(Buffer.from(text).equals(bytes), "SOURCE_ENCODING_UNSUPPORTED");
  requireV2(nativeGitV2(root, "show", `${head}:${path}`) === text.trim(), "SOURCE_DIFFERS_FROM_HEAD");
  requireV2(nativeGitV2(root, "hash-object", "--no-filters", "--", path) === entry.split(/\s+/)[2], "SOURCE_DIFFERS_FROM_HEAD");
  return text;
}

export class PlanningFreshnessV2 implements FreshnessV2 {
  readonly root: string;
  constructor(root: string) { this.root = root; }
  async hydrate(selector: NonNullable<PlanV2["source"]["selector"]>, requested: string[], scopeSummary: string): Promise<Pick<PlanV2, "repository" | "source">> {
    validateShapeV2(SourceScopeV2Schema, selector);
    const binding = await bindGitV2(this.root);
    assertGitAttributesV2(this.root);
    const baselineCommit = nativeGitV2(this.root, "rev-parse", "HEAD^{commit}"), baselineTree = nativeGitV2(this.root, "rev-parse", "HEAD^{tree}");
    const targetBranch = nativeGitV2(this.root, "symbolic-ref", "--no-recurse", "HEAD"); requireV2(targetBranch.startsWith("refs/heads/"), "LOCAL_BRANCH_REQUIRED");
    await assertTargetV2({ binding, targetRef: targetBranch }, { commit: baselineCommit, tree: baselineTree });
    const model = JSON.parse(await tracked(this.root, baselineCommit, "project-model/model.json"));
    assertValidProjectModel(model); requireV2(model.project.mode === "authoritative", "AUTHORITATIVE_MODEL_REQUIRED");
    requireV2(selector.workstreamIds.every(id => model.workstreams.some((w: any) => w.id === id)), "SOURCE_WORKSTREAM_MISSING");
    const eligible = specEligibleObjectIds(model), objects = allObjects(model);
    const accepted = objects.filter(({ object }) => eligible.has(object.id));
    const neighbors = new Map(accepted.map(({ object }) => [object.id, new Set<string>()]));
    for (const { object } of accepted) for (const relation of object.relationships) if (!["supersedes", "related_to"].includes(relation.kind) && eligible.has(relation.targetId)) {
      // Governing links are conservative and bidirectional: a newly accepted
      // off-scope constraint that affects a selected object is applicable too.
      neighbors.get(object.id)!.add(relation.targetId); neighbors.get(relation.targetId)!.add(object.id);
    }
    const selected = new Set(accepted.filter(({ object }) => object.scope.kind === "repository" || object.scope.workstreamIds.some(id => selector.workstreamIds.includes(id))).map(({ object }) => object.id));
    const queue = [...selected];
    for (let i = 0; i < queue.length; i++) for (const id of neighbors.get(queue[i])!) if (!selected.has(id)) { selected.add(id); queue.push(id); }
    const governingCollections = new Set(["intents", "concepts", "scenarios", "decisions", "commitments"]);
    for (const { object } of accepted.filter(e => selected.has(e.object.id))) for (const relation of object.relationships.filter(r => r.kind === "depends_on")) {
      const target = objects.find(e => e.object.id === relation.targetId)!;
      requireV2(!governingCollections.has(target.collection) || eligible.has(target.object.id), `GOVERNING_DEPENDENCY_NOT_ACCEPTED: ${relation.targetId}`);
    }
    const closure = accepted.filter(({ object }) => selected.has(object.id))
      .map(({ collection, object }) => ({ ref: `model:${collection}/${object.id}`, digest: semanticHash(collection, object) })).sort((a, b) => a.ref.localeCompare(b.ref));
    requireV2(closure.length > 0, "GOVERNING_SOURCE_REQUIRED");
    const refs = new Map(closure.map(r => [r.ref, r]));
    const rendered = new SpecProjector(this.root).render(model);
    for (const ref of requested) {
      if (ref.startsWith("model:")) {
        const match = objects.find(({ collection, object }) => ref === `model:${collection}/${object.id}`);
        requireV2(match, `SOURCE_NOT_FOUND: ${ref}`);
        if (governingCollections.has(match.collection)) requireV2(refs.has(ref), `SOURCE_NOT_GOVERNING_SELECTED_SCOPE: ${ref}`);
        refs.set(ref, { ref, digest: semanticHash(match.collection, match.object) });
      } else if (ref.startsWith("spec:")) {
        const path = ref.slice(5), matches = rendered.filter(p => p.path === path);
        requireV2(matches.length === 1, `SPEC_NOT_EXACT: ${path}`);
        const actual = await tracked(this.root, baselineCommit, path);
        requireV2(actual === matches[0].content, `SPEC_PROJECTION_STALE: ${path}`);
        refs.set(ref, { ref, digest: digest(actual) });
      } else throw Error(`UNSUPPORTED_SOURCE_REFERENCE: ${ref}`);
    }
    await assertTargetV2({ binding, targetRef: targetBranch }, { commit: baselineCommit, tree: baselineTree });
    return { repository: { repositoryId: `repo-${canonicalHash(binding).slice(7)}`, baselineCommit, baselineTree, targetBranch },
      source: { selector: structuredClone(selector), governingClosure: canonicalHash({ projectId: model.project.id, selector, closure }), refs: [...refs.values()].sort((a, b) => a.ref.localeCompare(b.ref)), scopeSummary } };
  }
  async observe(plan: Readonly<PlanV2>): Promise<Omit<AcceptanceV2, "observedAt">> {
    const binding = await bindGitV2(this.root);
    assertGitAttributesV2(this.root);
    const repository = { repositoryId: `repo-${canonicalHash(binding).slice(7)}`,
      baselineCommit: nativeGitV2(this.root, "rev-parse", "HEAD^{commit}"),
      baselineTree: nativeGitV2(this.root, "rev-parse", "HEAD^{tree}"),
      targetBranch: nativeGitV2(this.root, "symbolic-ref", "--no-recurse", "HEAD") };
    requireV2(repository.repositoryId === plan.repository.repositoryId, "PRODUCT_NATIVE_REPOSITORY_DRIFT");
    requireV2(repository.targetBranch.startsWith("refs/heads/"), "LOCAL_BRANCH_REQUIRED");
    await assertTargetV2({ binding, targetRef: repository.targetBranch }, { commit: repository.baselineCommit, tree: repository.baselineTree });
    // Semantic hydration can stop at the first unavailable source. Check every
    // retained path independently so that observation cannot hide an unsafe one.
    const paths = ["project-model/model.json", ...plan.source.refs.filter(r => r.ref.startsWith("spec:")).map(r => r.ref.slice(5))];
    for (const path of paths) {
      try { await tracked(this.root, repository.baselineCommit, path); }
      catch (error) {
        if (!(error instanceof Error && error.message.startsWith("SOURCE_NOT_FOUND: "))) throw error;
      }
    }
    const findings: string[] = [];
    try {
      const model = JSON.parse(await tracked(this.root, repository.baselineCommit, "project-model/model.json"));
      assertValidProjectModel(model);
      const workstreamIds = plan.source.selector?.workstreamIds ?? [];
      for (const { collection, object } of allObjects(model)) {
        const relevant = object.scope.kind === "repository" || object.scope.workstreamIds.some(id => workstreamIds.includes(id));
        const unresolved = (collection === "questions" && ["open", "deferred"].includes(object.state))
          || (collection === "tensions" && ["active", "deferred"].includes(object.state))
          || (["decisions", "proposals", "intents", "concepts", "scenarios", "commitments"].includes(collection) && ["candidate", "proposed", "not_reviewed"].includes(object.state));
        if (relevant && unresolved) findings.push(`REVIEW_REQUIRED: ${collection}/${object.id} (${object.state}): ${object.title}; resolve material uncertainty using conversation before dispatch`);
      }
      for (const review of model.project.reviews ?? []) {
        if (review.status === "pending" && (!review.scope.workstreamIds.length || review.scope.workstreamIds.some((id: string) => workstreamIds.includes(id)))) {
          findings.push(`REVIEW_REQUIRED: ${review.id} revision ${review.revision}: ${review.title}; inspect pending outcomes before dispatch`);
        }
      }
    } catch (error) {
      if (!(error instanceof Error && error.message.startsWith("SOURCE_NOT_FOUND: "))) throw error;
    }
    let source: PlanV2["source"] | undefined;
    try {
      const current = await this.current(plan);
      requireV2(sameV2(current.repository, repository), "ACCEPTANCE_TARGET_RACE");
      source = current.source;
    } catch (error) {
      // Only semantic provenance failures are observations. Corruption, unsafe
      // paths, dirty native state, unsupported formats and unexpected I/O stay hard.
      // A committed source deletion is unavailable provenance, not fresh content.
      const message = error instanceof Error ? error.message : String(error);
      requireV2(/^(AUTHORITATIVE_MODEL_REQUIRED|SOURCE_WORKSTREAM_MISSING|GOVERNING_DEPENDENCY_NOT_ACCEPTED|GOVERNING_SOURCE_REQUIRED|SOURCE_NOT_FOUND|SOURCE_NOT_GOVERNING_SELECTED_SCOPE|SPEC_NOT_EXACT|SPEC_PROJECTION_STALE)(:|$)/.test(message), message);
      findings.push(`SOURCE_ASSESSMENT: ${message}`);
    }
    await assertTargetV2({ binding, targetRef: repository.targetBranch }, { commit: repository.baselineCommit, tree: repository.baselineTree });
    return { repository, ...(source ? { source } : {}), findings };
  }
  async current(plan: Readonly<PlanV2>) {
    requireV2(plan.source.selector, "PRODUCT_SOURCE_SELECTOR_REQUIRED");
    return this.hydrate(plan.source.selector, plan.source.refs.map(r => r.ref), plan.source.scopeSummary);
  }
}
