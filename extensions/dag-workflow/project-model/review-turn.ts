import { resolve, sep } from "node:path";
import { sha256 } from "./model.ts";
import type { ReviewPoint } from "./types.ts";

export const TURN_PROJECTION_SCHEMA_VERSION = 1 as const;

export interface PresentationBlock {
  id: string;
  placement: "before-review" | "after-review" | `point:${string}`;
  kind: "markdown" | "callout" | "code" | "table" | "comparison" | "html";
  title?: string;
  body?: string;
  tone?: string;
  code?: string;
  headers?: string[];
  rows?: string[][];
  intro?: string;
  columns?: Array<{ title: string; items: string[] }>;
  html?: string;
}

export interface ModelReviewTurnProjection {
  schemaVersion: typeof TURN_PROJECTION_SCHEMA_VERSION;
  project: { id: string; title: string; revision: number; modelHash: string };
  focus: { id: string; title: string; workstreamIds: string[] };
  currentUnderstanding: { body: string };
  delta: { added: number; changed: number; stillUnresolved: number; possibleMisunderstanding?: string; consequences: string[] };
  frontier: Array<{ id: string; type: string; title: string; state: string; summary: string; badges: string[] }>;
  frontierHandoff?: string;
  review: { id: string; title: string; semanticHash: string; points: ReviewPoint[] };
  presentationBlocks?: PresentationBlock[];
}

export function reviewSemanticHash(review: { id: string; title: string; points: ReviewPoint[] }): string {
  return sha256({ id: review.id, title: review.title, points: review.points });
}

export function validateTurnProjection(value: unknown): string[] {
  const errors: string[] = [];
  if (!value || typeof value !== "object" || Array.isArray(value)) return ["projection must be an object"];
  const projection = value as Partial<ModelReviewTurnProjection>;
  if (projection.schemaVersion !== TURN_PROJECTION_SCHEMA_VERSION) errors.push(`schemaVersion must be ${TURN_PROJECTION_SCHEMA_VERSION}`);
  requireRecord(projection.project, "project", errors);
  requireString(projection.project?.id, "project.id", errors);
  requireString(projection.project?.title, "project.title", errors);
  if (!Number.isInteger(projection.project?.revision) || Number(projection.project?.revision) < 0) errors.push("project.revision must be a non-negative integer");
  requireString(projection.project?.modelHash, "project.modelHash", errors);
  requireRecord(projection.focus, "focus", errors);
  requireSafeId(projection.focus?.id, "focus.id", errors);
  requireString(projection.focus?.title, "focus.title", errors);
  stringArray(projection.focus?.workstreamIds, "focus.workstreamIds", errors);
  requireRecord(projection.currentUnderstanding, "currentUnderstanding", errors);
  requireString(projection.currentUnderstanding?.body, "currentUnderstanding.body", errors);
  requireRecord(projection.review, "review", errors);
  requireSafeId(projection.review?.id, "review.id", errors);
  requireString(projection.review?.title, "review.title", errors);
  requireString(projection.review?.semanticHash, "review.semanticHash", errors);

  const points = array(projection.review?.points, "review.points", errors) as ReviewPoint[];
  const pointIds = new Set<string>();
  for (const [index, point] of points.entries()) {
    const label = `review.points[${index}]`;
    requireRecord(point, label, errors);
    requireSafeId(point?.id, `${label}.id`, errors);
    if (pointIds.has(point?.id)) errors.push(`duplicate review point id: ${point?.id}`);
    pointIds.add(point?.id);
    requireString(point?.title, `${label}.title`, errors);
    requireString(point?.context, `${label}.context`, errors);
    if (!new Set(["awareness", "decision"]).has(point?.purpose)) errors.push(`${label}.purpose is invalid`);
    if (point?.purpose === "decision") requireString(point?.question, `${label}.question`, errors);
    const options = array(point?.options, `${label}.options`, errors) as ReviewPoint["options"];
    if (point?.purpose === "decision" && !options.length) errors.push(`${label} decision requires options`);
    const optionIds = new Set<string>();
    for (const [optionIndex, option] of options.entries()) {
      const optionLabel = `${label}.options[${optionIndex}]`;
      requireRecord(option, optionLabel, errors);
      requireSafeId(option?.id, `${optionLabel}.id`, errors);
      if (optionIds.has(option?.id)) errors.push(`duplicate option id in ${point?.id}: ${option?.id}`);
      optionIds.add(option?.id);
      requireString(option?.label, `${optionLabel}.label`, errors);
      requireString(option?.description, `${optionLabel}.description`, errors);
      requireString(option?.semanticHash, `${optionLabel}.semanticHash`, errors);
    }
  }

  const frontier = projection.frontier === undefined ? [] : array(projection.frontier, "frontier", errors);
  for (const [index, item] of frontier.entries()) {
    requireRecord(item, `frontier[${index}]`, errors);
    requireSafeId((item as any)?.id, `frontier[${index}].id`, errors);
    requireString((item as any)?.title, `frontier[${index}].title`, errors);
  }

  const blocks = projection.presentationBlocks === undefined ? [] : array(projection.presentationBlocks, "presentationBlocks", errors) as PresentationBlock[];
  const blockIds = new Set<string>();
  for (const [index, block] of blocks.entries()) {
    const label = `presentationBlocks[${index}]`;
    requireRecord(block, label, errors);
    requireSafeId(block?.id, `${label}.id`, errors);
    if (blockIds.has(block?.id)) errors.push(`duplicate presentation block id: ${block?.id}`);
    blockIds.add(block?.id);
    if (!new Set(["markdown", "callout", "code", "table", "comparison", "html"]).has(block?.kind)) errors.push(`${label}.kind is invalid`);
    requireString(block?.placement, `${label}.placement`, errors);
    if (String(block?.placement).startsWith("point:") && !pointIds.has(String(block.placement).slice(6))) errors.push(`${label} references an unknown point placement`);
  }
  return errors;
}

export function assertTurnProjection(value: unknown): asserts value is ModelReviewTurnProjection {
  const errors = validateTurnProjection(value);
  if (errors.length) throw new Error(`Invalid ModelReviewTurnProjection:\n- ${errors.join("\n- ")}`);
}

export function reviewArtifactPaths(root: string, focusId: string, reviewId: string) {
  requireSafeSegment(focusId, "focusId");
  requireSafeSegment(reviewId, "reviewId");
  const base = resolve(root, ".ai", "model-sessions", focusId, "lavish");
  const html = resolve(base, `${reviewId}.html`);
  const metadata = resolve(base, `${reviewId}.presentation.json`);
  for (const path of [html, metadata]) if (path !== base && !path.startsWith(`${base}${sep}`)) throw new Error(`Unsafe artifact path: ${path}`);
  return { base, html, metadata };
}

function requireSafeSegment(value: string, label: string) {
  if (typeof value !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(value)) throw new Error(`${label} must be a safe identifier`);
}
function requireSafeId(value: unknown, label: string, errors: string[]) {
  if (typeof value !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(value)) errors.push(`${label} must be a safe identifier`);
}
function requireString(value: unknown, label: string, errors: string[]) {
  if (typeof value !== "string" || !value.trim()) errors.push(`${label} is required`);
}
function requireRecord(value: unknown, label: string, errors: string[]) {
  if (!value || typeof value !== "object" || Array.isArray(value)) errors.push(`${label} must be an object`);
}
function stringArray(value: unknown, label: string, errors: string[]) {
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) errors.push(`${label} must be a string array`);
}
function array(value: unknown, label: string, errors: string[]): unknown[] {
  if (!Array.isArray(value)) { errors.push(`${label} must be an array`); return []; }
  return value;
}
