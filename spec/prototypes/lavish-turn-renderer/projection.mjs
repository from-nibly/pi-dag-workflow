import { resolve, sep } from "node:path";

export const TURN_PROJECTION_SCHEMA_VERSION = 1;

export function validateTurnProjection(value) {
  const errors = [];
  if (!value || typeof value !== "object" || Array.isArray(value)) return ["projection must be an object"];
  if (value.schemaVersion !== TURN_PROJECTION_SCHEMA_VERSION) errors.push(`schemaVersion must be ${TURN_PROJECTION_SCHEMA_VERSION}`);
  if (value.statusLabel !== undefined) requireString(value.statusLabel, "statusLabel", errors);
  if (value.presentationNotice !== undefined) requireString(value.presentationNotice, "presentationNotice", errors);
  requireRecord(value.project, "project", errors);
  requireString(value.project?.id, "project.id", errors);
  requireString(value.project?.title, "project.title", errors);
  if (!Number.isInteger(value.project?.revision) || value.project.revision < 0) errors.push("project.revision must be a non-negative integer");
  requireString(value.project?.modelHash, "project.modelHash", errors);
  requireRecord(value.focus, "focus", errors);
  requireSafeId(value.focus?.id, "focus.id", errors);
  requireString(value.focus?.title, "focus.title", errors);
  stringArray(value.focus?.workstreamIds, "focus.workstreamIds", errors);
  requireRecord(value.currentUnderstanding, "currentUnderstanding", errors);
  requireString(value.currentUnderstanding?.body, "currentUnderstanding.body", errors);
  requireRecord(value.review, "review", errors);
  requireSafeId(value.review?.id, "review.id", errors);
  requireString(value.review?.title, "review.title", errors);
  requireString(value.review?.semanticHash, "review.semanticHash", errors);

  const points = array(value.review?.points, "review.points", errors);
  const pointIds = new Set();
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
    const options = array(point?.options, `${label}.options`, errors);
    if (point?.purpose === "decision" && !options.length) errors.push(`${label} decision requires options`);
    const optionIds = new Set();
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

  if (value.frontierHandoff !== undefined) requireString(value.frontierHandoff, "frontierHandoff", errors);
  const frontier = value.frontier === undefined ? [] : array(value.frontier, "frontier", errors);
  for (const [index, item] of frontier.entries()) {
    requireRecord(item, `frontier[${index}]`, errors);
    requireSafeId(item?.id, `frontier[${index}].id`, errors);
    requireString(item?.title, `frontier[${index}].title`, errors);
  }

  const blocks = value.presentationBlocks === undefined ? [] : array(value.presentationBlocks, "presentationBlocks", errors);
  const blockIds = new Set();
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

export function assertTurnProjection(value) {
  const errors = validateTurnProjection(value);
  if (errors.length) throw new Error(`Invalid ModelReviewTurnProjection:\n- ${errors.join("\n- ")}`);
  return value;
}

export function artifactPaths(root, focusId, reviewId) {
  requireSafeSegment(focusId, "focusId");
  requireSafeSegment(reviewId, "reviewId");
  const base = resolve(root, ".ai", "model-sessions", focusId, "lavish");
  const html = resolve(base, `${reviewId}.html`);
  const metadata = resolve(base, `${reviewId}.presentation.json`);
  for (const path of [html, metadata]) if (path !== base && !path.startsWith(`${base}${sep}`)) throw new Error(`Unsafe artifact path: ${path}`);
  return { base, html, metadata };
}

function requireSafeSegment(value, label) {
  if (typeof value !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(value)) throw new Error(`${label} must be a safe identifier`);
}
function requireSafeId(value, label, errors) {
  if (typeof value !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(value)) errors.push(`${label} must be a safe identifier`);
}
function requireString(value, label, errors) {
  if (typeof value !== "string" || !value.trim()) errors.push(`${label} is required`);
}
function requireRecord(value, label, errors) {
  if (!value || typeof value !== "object" || Array.isArray(value)) errors.push(`${label} must be an object`);
}
function stringArray(value, label, errors) {
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) errors.push(`${label} must be a string array`);
}
function array(value, label, errors) {
  if (!Array.isArray(value)) { errors.push(`${label} must be an array`); return []; }
  return value;
}
