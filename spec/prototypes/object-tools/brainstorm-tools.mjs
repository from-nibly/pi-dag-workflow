import { access, mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

const COLLECTIONS = [
  "neighborhoods",
  "tangents",
  "questions",
  "evidence",
  "proposals",
  "probes",
  "decisions",
  "reviews",
  "promotions",
];
const ADDABLE_COLLECTIONS = new Set(["neighborhoods", "tangents", "questions", "evidence", "proposals", "probes"]);
const PATCHABLE_COLLECTIONS = new Set([...ADDABLE_COLLECTIONS, "decisions", "promotions"]);
const PREFIXES = {
  neighborhoods: "N",
  tangents: "T",
  questions: "Q",
  evidence: "E",
  proposals: "P",
  probes: "B",
  decisions: "D",
  reviews: "R",
  promotions: "M",
};
const SIGNAL_LABELS = {
  needs_clarity: "Needs clarity",
  foundation: "Foundation",
  risk: "Risk",
  integration: "Integration",
  prototype_candidate: "Prototype candidate",
};
const SIGNAL_TYPES = new Set(Object.keys(SIGNAL_LABELS));
const QUESTION_STATUSES = new Set(["open", "answered", "deferred", "obsolete", "blocked"]);
const TANGENT_STATUSES = new Set(["proposed", "tracked", "active", "deferred", "out_of_scope", "closed"]);

export class BrainstormPrototype {
  constructor(root) {
    this.root = resolve(root);
  }

  async execute(name, params = {}) {
    const handler = TOOL_HANDLERS[name];
    if (!handler) throw new Error(`Unknown brainstorm tool: ${name}`);
    return handler(this, params);
  }

  statePath(id) { return join(this.root, ".ai", "brainstorm", `${id}.json`); }

  async start({ id, title }) {
    requiredString(id, "id");
    requiredString(title, "title");
    try {
      const existing = await this.load(id);
      return { id, revision: existing.revision, statePath: this.statePath(id), resumed: true };
    } catch (error) {
      if (!String(error?.message).startsWith("Brainstorm not found:")) throw error;
    }
    const createdAt = now();
    const state = {
      schemaVersion: 2,
      id,
      title,
      revision: 0,
      createdAt,
      updatedAt: createdAt,
      currentUnderstanding: null,
      ...Object.fromEntries(COLLECTIONS.map((name) => [name, []])),
    };
    await this.save(state);
    return { id, revision: 0, statePath: this.statePath(id), resumed: false };
  }

  async load(id) {
    try {
      return upgradeState(JSON.parse(await readFile(this.statePath(id), "utf8")));
    } catch (error) {
      if (error?.code === "ENOENT") throw new Error(`Brainstorm not found: ${id}`);
      throw error;
    }
  }

  async save(state) {
    state.updatedAt = now();
    const path = this.statePath(state.id);
    await mkdir(dirname(path), { recursive: true });
    const temporary = `${path}.${process.pid}.tmp`;
    await writeFile(temporary, `${JSON.stringify(state, null, 2)}\n`, "utf8");
    await rename(temporary, path);
  }

  async mutate(id, mutate) {
    const state = await this.load(id);
    const changed = createChangeSet();
    await mutate(state, changed);
    const errors = validateState(state);
    if (errors.length) throw new Error(`Invalid brainstorm state:\n- ${errors.join("\n- ")}`);
    state.revision += 1;
    await this.save(state);
    return { revision: state.revision, state, changed };
  }
}

const TOOL_HANDLERS = {
  async dag_brainstorm_context(runtime, params) {
    const state = await runtime.load(params.id);
    const view = params.view ?? "orientation";
    const data = projectContext(state, view, params);
    return axiResult("context", state, createChangeSet(), data);
  },

  async dag_brainstorm_update(runtime, params) {
    const result = await runtime.mutate(params.id, async (state, changed) => {
      applyDomainUpdate(state, params, changed);
      if (params.understanding) setCurrentUnderstanding(state, params.understanding, { type: "update" }, changed);
    });
    return axiResult("update", result.state, result.changed);
  },

  async dag_brainstorm_review(runtime, params) {
    const result = await runtime.mutate(params.id, async (state, changed) => {
      if (params.add || params.patch || params.remove || params.signals) applyDomainUpdate(state, params, changed);
      if (params.understanding) setCurrentUnderstanding(state, params.understanding, { type: state.lastResolution ? "update" : "orientation" }, changed);
      if (!state.currentUnderstanding) throw new Error("Review requires an Initial or Current understanding proof");
      const active = state.reviews.find(({ status }) => status === "reviewing");
      if (active && !params.replace) throw new Error(`Active review already exists: ${active.id}`);
      if (active) {
        state.reviews = state.reviews.filter(({ id }) => id !== active.id);
        changed.removed.push(active.id);
      }

      const review = {
        id: allocateId(state, "reviews", { id: params.reviewId, key: params.key, title: params.title }),
        key: params.key,
        title: params.title,
        status: "reviewing",
        createdAt: now(),
        points: [],
      };
      requiredString(review.title, "review.title");

      for (const input of params.points ?? []) {
        const questionIds = resolveMany(state, "questions", input.questionIds ?? input.questions ?? (input.question ? [input.question] : []));
        if (!questionIds.length && input.purpose !== "informational") throw new Error("Review point requires at least one question reference");
        const point = {
          id: allocateNestedId(state, review.points, "RP", input),
          key: input.key,
          title: input.title,
          context: input.context,
          purpose: input.purpose,
          questionIds,
          question: input.prompt ?? inferQuestionPrompt(state, questionIds),
          proposalIds: resolveMany(state, "proposals", input.proposalIds ?? input.proposals ?? []),
          recommendedProposalId: input.recommendedProposalId
            ? resolveRef(state, "proposals", input.recommendedProposalId).id
            : undefined,
          recommendationRationale: input.recommendationRationale,
          status: "presented",
        };
        requiredString(point.title, `${point.id}.title`);
        requiredString(point.context, `${point.id}.context`);
        if (point.purpose !== "informational") requiredString(point.question, `${point.id}.question`);

        for (const option of input.options ?? []) {
          const proposal = normalizeNewEntity(state, "proposals", {
            ...option,
            key: option.key,
            title: option.title ?? option.label,
            body: option.body ?? option.description,
            description: option.description ?? option.body,
            questionIds,
            status: option.recommended ? "recommended" : (option.status ?? "candidate"),
          });
          state.proposals.push(proposal);
          changed.added.push(proposal.id);
          point.proposalIds.push(proposal.id);
          if (option.recommended) {
            point.recommendedProposalId = proposal.id;
            point.recommendationRationale = option.rationale ?? point.recommendationRationale;
          }
        }
        point.proposalIds = [...new Set(point.proposalIds)];
        if (point.recommendedProposalId && !point.proposalIds.includes(point.recommendedProposalId)) {
          throw new Error(`${point.id} recommends a proposal that is not one of its options`);
        }
        if (point.recommendedProposalId) {
          const proposal = requireEntity(state.proposals, point.recommendedProposalId, "proposal");
          requiredString(point.recommendationRationale ?? proposal.rationale, `${point.id}.recommendationRationale`);
        }
        review.points.push(point);
      }
      if (!review.points.length) throw new Error("Review requires at least one point");
      state.reviews.push(review);
      changed.added.push(review.id);
    });
    const review = result.state.reviews.find(({ id }) => result.changed.added.includes(id));
    return axiResult("review", result.state, result.changed, projectReviewReceipt(review), [
      "Present the review packet as ordinary Markdown or render it with optional Lavish.",
      `After the user responds, call dag_brainstorm_resolve_review for ${review.id}.`,
    ]);
  },

  async dag_brainstorm_resolve_review(runtime, params) {
    const result = await runtime.mutate(params.id, async (state, changed) => {
      const review = params.reviewId
        ? requireEntity(state.reviews, params.reviewId, "review")
        : state.reviews.find(({ status }) => status === "reviewing");
      if (!review) throw new Error("No active review to resolve");

      const explicit = new Map((params.outcomes ?? []).map((outcome) => [outcome.pointId ?? outcome.point, outcome]));
      if (params.acceptAllRecommended) {
        for (const point of review.points) {
          if (!point.recommendedProposalId || (params.exceptPointIds ?? []).includes(point.id) || explicit.has(point.id)) continue;
          explicit.set(point.id, { pointId: point.id, action: "accept", proposalId: point.recommendedProposalId });
        }
      }

      const decisionIds = [];
      for (const point of review.points) {
        const outcome = explicit.get(point.id) ?? (point.key ? explicit.get(point.key) : undefined);
        if (!outcome || outcome.action === "unresolved") continue;
        const decision = decisionFromOutcome(state, point, outcome);
        state.decisions.push(decision);
        changed.added.push(decision.id);
        decisionIds.push(decision.id);

        for (const questionId of point.questionIds) {
          const question = requireEntity(state.questions, questionId, "question");
          question.status = "answered";
          question.resolvedByDecisionIds = [...new Set([...(question.resolvedByDecisionIds ?? []), decision.id])];
          question.updatedAt = now();
          changed.updated.push(question.id);
        }
        if (decision.proposalIds.length) {
          const selected = decision.proposalIds[0];
          for (const proposalId of point.proposalIds) {
            const proposal = requireEntity(state.proposals, proposalId, "proposal");
            proposal.status = proposalId === selected ? (outcome.action === "modify" ? "modified" : "accepted") : (outcome.rejectAlternatives === false ? proposal.status : "rejected");
            proposal.updatedAt = now();
            changed.updated.push(proposal.id);
          }
        }
      }

      if (params.add || params.patch || params.remove || params.signals) applyDomainUpdate(state, params, changed);
      for (const tangent of state.tangents.filter(({ status }) => status === "proposed")) {
        tangent.status = "tracked";
        tangent.updatedAt = now();
        changed.updated.push(tangent.id);
      }

      state.lastResolution = {
        reviewId: review.id,
        resolvedAt: now(),
        decisionIds,
        unresolvedPointIds: review.points.filter((point) => point.purpose !== "informational" && !explicit.has(point.id) && !(point.key && explicit.has(point.key))).map(({ id }) => id),
      };
      if ((decisionIds.length || params.understanding) && !params.understanding) {
        throw new Error("A semantic review resolution requires a Current understanding proof");
      }
      if (params.understanding) setCurrentUnderstanding(state, params.understanding, {
        type: "resolution",
        reviewId: review.id,
        decisionIds,
      }, changed);
      state.reviews = state.reviews.filter(({ id }) => id !== review.id);
      changed.removed.push(review.id);
    });
    return axiResult("resolve_review", result.state, result.changed, {
      resolution: result.state.lastResolution,
      frontier: projectFrontier(result.state),
    });
  },

  async dag_brainstorm_promote(runtime, params) {
    const state = await runtime.load(params.id);
    const action = params.action ?? "prepare";
    if (action === "prepare") {
      const decisionIds = params.decisionIds ?? [];
      if (!decisionIds.length) throw new Error("Promotion prepare requires decisionIds");
      const decisions = decisionIds.map((id) => requireEntity(state.decisions, id, "decision"));
      return axiResult("promote.prepare", state, createChangeSet(), {
        decisions: decisions.map(({ id, title, contract, rationale, questionIds, proposalIds }) => ({ id, title, contract, rationale, questionIds, proposalIds })),
        targetHints: params.targetHints ?? [],
      }, [
        "Read the relevant canonical spec entry and linked supporting documents.",
        "Edit canonical Markdown with normal coding tools.",
        "Call dag_brainstorm_promote with action=record and the changed spec paths.",
      ]);
    }
    if (action !== "record") throw new Error(`Unknown promotion action: ${action}`);

    const result = await runtime.mutate(params.id, async (mutable, changed) => {
      const targets = structuredClone(params.targets ?? []);
      if (!targets.length) throw new Error("Promotion record requires targets");
      const decisionIds = [...new Set(targets.flatMap(({ decisionIds = [] }) => decisionIds))];
      if (!decisionIds.length) throw new Error("Promotion record requires decisionIds");
      for (const id of decisionIds) requireEntity(mutable.decisions, id, "decision");
      for (const target of targets) {
        requiredString(target.path, "promotion target.path");
        if (!(target.decisionIds?.length > 0)) throw new Error(`Promotion target ${target.path} requires decisionIds`);
        const absolute = resolve(runtime.root, target.path);
        if (!isWithin(runtime.root, absolute)) throw new Error("Promotion target must stay inside the project root");
        await access(absolute);
      }
      const promotion = {
        id: allocateId(mutable, "promotions", { id: params.promotionId, key: params.key, title: params.title }),
        key: params.key,
        decisionIds,
        targets,
        createdAt: now(),
      };
      mutable.promotions.push(promotion);
      changed.added.push(promotion.id);
    });
    return axiResult("promote.record", result.state, result.changed);
  },
};

function upgradeState(raw) {
  const state = structuredClone(raw);
  if (state.schemaVersion === 1 || state.turns || state.feedback) {
    state.reviews = state.reviews ?? state.turns ?? [];
    for (const review of state.reviews) {
      review.points ??= review.discussionPoints ?? [];
      delete review.discussionPoints;
      if (review.status === "draft") review.status = "reviewing";
    }
    delete state.turns;
    delete state.feedback;
    if (state.lastIntegration && !state.lastResolution) {
      state.lastResolution = {
        reviewId: state.lastIntegration.turnId,
        resolvedAt: state.lastIntegration.integratedAt,
        decisionIds: state.lastIntegration.decisionIds ?? [],
        unresolvedPointIds: [],
      };
    }
    delete state.lastIntegration;
    state.schemaVersion = 2;
  }
  state.currentUnderstanding ??= null;
  for (const name of COLLECTIONS) state[name] ??= [];
  return state;
}

function setCurrentUnderstanding(state, input, sourceDefaults, changed) {
  const existed = Boolean(state.currentUnderstanding);
  const body = typeof input === "string" ? input : input.body;
  requiredString(body, "understanding.body");
  const source = { ...sourceDefaults };
  if (source.type === "update") {
    source.evidenceIds = resolveMany(state, "evidence", input.evidenceIds ?? input.evidence ?? []);
    source.decisionIds = resolveMany(state, "decisions", input.decisionIds ?? input.decisions ?? []);
    if (!source.evidenceIds.length && !source.decisionIds.length) throw new Error("Updated Current understanding requires evidence or decision references");
  }
  state.currentUnderstanding = {
    label: source.type === "orientation" ? "Initial understanding" : "Current understanding",
    body,
    source,
    updatedAt: now(),
  };
  changed[existed ? "updated" : "added"].push("currentUnderstanding");
}

function applyDomainUpdate(state, params, changed) {
  const add = params.add ?? {};
  const order = ["neighborhoods", "tangents", "evidence", "questions", "proposals", "probes"];
  for (const collection of order) {
    for (const input of add[collection] ?? []) {
      const entity = normalizeNewEntity(state, collection, input);
      state[collection].push(entity);
      changed.added.push(entity.id);
    }
  }

  for (const patch of params.patch ?? []) {
    if (!PATCHABLE_COLLECTIONS.has(patch.collection)) throw new Error(`Cannot patch collection: ${patch.collection}`);
    const entity = resolveRef(state, patch.collection, patch.id ?? patch.ref);
    const changes = normalizeReferences(state, patch.collection, structuredClone(patch.changes ?? {}));
    Object.assign(entity, changes, { updatedAt: now() });
    changed.updated.push(entity.id);
  }

  for (const removal of params.remove ?? []) {
    if (!ADDABLE_COLLECTIONS.has(removal.collection)) throw new Error(`Cannot remove collection: ${removal.collection}`);
    const entity = resolveRef(state, removal.collection, removal.id ?? removal.ref);
    state[removal.collection] = state[removal.collection].filter(({ id }) => id !== entity.id);
    changed.removed.push(entity.id);
  }

  for (const signal of params.signals ?? []) {
    const tangent = resolveRef(state, "tangents", signal.tangentId ?? signal.tangent);
    if (!SIGNAL_TYPES.has(signal.type)) throw new Error(`Invalid signal type: ${signal.type}`);
    requiredString(signal.rationale, "signal.rationale");
    tangent.signals ??= [];
    tangent.signals = tangent.signals.filter(({ type }) => type !== signal.type);
    tangent.signals.push({ type: signal.type, rationale: signal.rationale, assessedAt: now() });
    tangent.updatedAt = now();
    changed.updated.push(tangent.id);
  }
}

function normalizeNewEntity(state, collection, input) {
  if (!ADDABLE_COLLECTIONS.has(collection)) throw new Error(`Cannot add collection: ${collection}`);
  const value = normalizeReferences(state, collection, structuredClone(input));
  value.id = allocateId(state, collection, value);
  value.createdAt ??= now();
  if (collection === "neighborhoods") value.status ??= "tracked";
  if (collection === "tangents") value.status ??= value.source === "user" ? "tracked" : "proposed";
  if (collection === "questions") value.status ??= "open";
  if (collection === "proposals") value.status ??= "candidate";
  if (collection === "probes") value.status ??= "planned";
  validateEntityForAdd(state, collection, value);
  return value;
}

function normalizeReferences(state, collection, value) {
  if (collection === "tangents") value.neighborhoodIds = resolveMany(state, "neighborhoods", value.neighborhoodIds ?? value.neighborhoods ?? []);
  if (collection === "questions") {
    value.tangentIds = resolveMany(state, "tangents", value.tangentIds ?? value.tangents ?? []);
    value.evidenceIds = resolveMany(state, "evidence", value.evidenceIds ?? value.evidence ?? []);
  }
  if (collection === "proposals" || collection === "probes") value.questionIds = resolveMany(state, "questions", value.questionIds ?? value.questions ?? []);
  delete value.neighborhoods;
  delete value.tangents;
  delete value.evidence;
  delete value.questions;
  return value;
}

function validateEntityForAdd(state, collection, value) {
  if (findById(state[collection], value.id)) throw new Error(`Duplicate ${collection} id: ${value.id}`);
  if (collection === "tangents") {
    requiredString(value.title, `${value.id}.title`);
    if (!TANGENT_STATUSES.has(value.status)) throw new Error(`Invalid tangent status: ${value.status}`);
    if (value.status === "proposed") {
      requiredString(value.whyNow, `${value.id}.whyNow`);
      requiredString(value.researchNeeded, `${value.id}.researchNeeded`);
      requiredString(value.connection, `${value.id}.connection`);
    }
  }
  if (collection === "questions") {
    requiredString(value.prompt, `${value.id}.prompt`);
    if (!QUESTION_STATUSES.has(value.status)) throw new Error(`Invalid question status: ${value.status}`);
    if (value.kind === "contradiction" && !(value.conflictsWith?.length >= 2)) throw new Error(`Contradiction ${value.id} requires at least two conflicting references`);
  }
  if (collection === "proposals") {
    requiredString(value.title, `${value.id}.title`);
    requiredString(value.description ?? value.body ?? value.contract, `${value.id}.description`);
  }
  if (collection === "evidence") requiredString(value.summary, `${value.id}.summary`);
}

function decisionFromOutcome(state, point, outcome) {
  if (outcome.action === "modify") {
    requiredString(outcome.title, "modified decision.title");
    requiredString(outcome.contract, "modified decision.contract");
    requiredString(outcome.rationale, "modified decision.rationale");
    return {
      id: allocateId(state, "decisions", { key: outcome.key ?? point.key ?? point.title }),
      title: outcome.title,
      contract: outcome.contract,
      rationale: outcome.rationale,
      disposition: outcome.disposition ?? "accepted",
      questionIds: point.questionIds,
      proposalIds: outcome.proposalId ? [resolveRef(state, "proposals", outcome.proposalId).id] : [],
      status: "active",
      createdAt: now(),
    };
  }
  if (outcome.action !== "accept") throw new Error(`Unknown outcome action: ${outcome.action}`);
  const proposalId = outcome.proposalId ?? outcome.proposal ?? point.recommendedProposalId;
  if (!proposalId) throw new Error(`Point ${point.id} acceptance requires proposalId or a recommendation`);
  const proposal = resolveRef(state, "proposals", proposalId);
  if (!point.proposalIds.includes(proposal.id)) throw new Error(`Proposal ${proposal.id} is not an option for ${point.id}`);
  const contract = outcome.contract ?? proposal.contract ?? proposal.description ?? proposal.body;
  const rationale = outcome.rationale ?? proposal.rationale ?? point.recommendationRationale;
  requiredString(contract, `${proposal.id}.contract`);
  requiredString(rationale, `${proposal.id}.rationale`);
  return {
    id: allocateId(state, "decisions", { key: outcome.key ?? proposal.key ?? proposal.title }),
    title: outcome.title ?? proposal.title,
    contract,
    rationale,
    disposition: outcome.disposition ?? "accepted",
    questionIds: point.questionIds,
    proposalIds: [proposal.id],
    status: "active",
    createdAt: now(),
  };
}

function projectContext(state, view, params) {
  if (view === "orientation") return {
    title: state.title,
    activeTangents: projectFrontier(state).filter(({ status }) => status === "active"),
    activeReview: state.reviews.find(({ status }) => status === "reviewing") ? projectReviewSummary(state.reviews.find(({ status }) => status === "reviewing")) : null,
    openQuestionCount: state.questions.filter(({ status }) => status === "open").length,
    frontier: projectFrontier(state),
    latestResolution: state.lastResolution ?? null,
    understanding: state.currentUnderstanding,
  };
  if (view === "frontier") return projectFrontier(state);
  if (view === "review") {
    const review = params.reviewId ? resolveRef(state, "reviews", params.reviewId) : state.reviews.find(({ status }) => status === "reviewing");
    return review ? projectReview(state, review) : null;
  }
  if (view === "entities") {
    if (!(params.ids?.length > 0)) throw new Error("entities view requires ids");
    return params.ids.map((id) => findEntityAcrossState(state, id));
  }
  if (view === "promotion_ready") {
    return state.decisions.filter((decision) => decision.status === "active" && isDecisionPromotionReady(state, decision)).map(({ id, title, disposition, questionIds }) => ({ id, title, disposition, questionIds }));
  }
  throw new Error(`Unknown context view: ${view}`);
}

function projectFrontier(state) {
  return state.tangents
    .filter(({ status }) => !["closed", "out_of_scope"].includes(status))
    .map((tangent) => ({
      id: tangent.id,
      title: tangent.title,
      status: tangent.status,
      badges: deriveTangentBadges(state, tangent),
      openQuestions: state.questions.filter((question) => question.status === "open" && question.tangentIds?.includes(tangent.id)).length,
      summary: tangent.explorationSummary ?? tangent.researchNeeded ?? "",
    }))
    .sort((a, b) => a.title.localeCompare(b.title));
}

function projectReview(state, review) {
  return {
    id: review.id,
    title: review.title,
    status: review.status,
    understanding: state.currentUnderstanding,
    points: review.points.map((point) => ({
      id: point.id,
      key: point.key,
      title: point.title,
      context: point.context,
      question: point.question,
      questionIds: point.questionIds,
      options: point.proposalIds.map((id) => {
        const proposal = requireEntity(state.proposals, id, "proposal");
        return {
          id: proposal.id,
          key: proposal.key,
          label: proposal.title,
          description: proposal.description ?? proposal.body ?? proposal.contract,
          recommended: id === point.recommendedProposalId,
          rationale: id === point.recommendedProposalId ? (point.recommendationRationale ?? proposal.rationale) : undefined,
        };
      }),
      other: true,
    })),
  };
}

function projectReviewReceipt(review) {
  return {
    id: review.id,
    title: review.title,
    pointCount: review.points.length,
    points: review.points.map((point) => ({
      id: point.id,
      key: point.key,
      title: point.title,
      questionIds: point.questionIds,
      options: point.proposalIds.map((id) => ({ id, recommended: id === point.recommendedProposalId })),
    })),
  };
}

function projectReviewSummary(review) {
  return { id: review.id, title: review.title, status: review.status, pointCount: review.points.length };
}

function axiResult(action, state, changed, data, nextOverride) {
  const next = nextOverride ?? deriveNextActions(state);
  return {
    ok: true,
    action,
    revision: state.revision,
    changed: compactChanges(changed),
    ...(data === undefined ? {} : { data }),
    next,
  };
}

function deriveNextActions(state) {
  const review = state.reviews.find(({ status }) => status === "reviewing");
  if (review) return [
    `Present ${review.id} as ordinary Markdown or render it with optional Lavish.`,
    `After the user responds, call dag_brainstorm_resolve_review for ${review.id}.`,
  ];
  const active = state.tangents.filter(({ status }) => status === "active");
  const open = state.questions.filter(({ status }) => status === "open");
  const promotionReady = state.decisions.filter((decision) => decision.status === "active" && isDecisionPromotionReady(state, decision));
  const next = [];
  if (active.length) next.push("Research the active tangent set and call dag_brainstorm_update when findings emerge.");
  if (open.length) next.push("Call dag_brainstorm_review when a coherent set of open questions is ready for user judgment.");
  if (!active.length) next.push("Show the updated tangent frontier and wait for the user to choose exploration focus.");
  if (promotionReady.length) next.push("Call dag_brainstorm_promote for settled decisions when a coherent canonical spec update is useful.");
  return next;
}

function isDecisionPromotionReady(state, decision) {
  const decisionChangedAt = Date.parse(decision.updatedAt ?? decision.createdAt ?? 0);
  const promotedAt = state.promotions
    .filter(({ decisionIds = [] }) => decisionIds.includes(decision.id))
    .reduce((latest, promotion) => Math.max(latest, Date.parse(promotion.createdAt ?? 0)), 0);
  return !promotedAt || decisionChangedAt > promotedAt;
}

function deriveTangentBadges(state, tangent) {
  const questions = state.questions.filter((question) => question.tangentIds?.includes(tangent.id));
  const open = questions.filter(({ status }) => status === "open");
  const badges = (tangent.signals ?? []).map(({ type }) => SIGNAL_LABELS[type]).filter(Boolean);
  if (tangent.status === "proposed") badges.push("Proposed");
  if (open.some(({ kind }) => kind === "contradiction")) badges.push("Contradiction");
  if (open.length) badges.push(`${open.length} open question${open.length === 1 ? "" : "s"}`);
  if (questions.some(({ status }) => status === "answered") && open.length) badges.push("Partially settled");
  if (tangent.status === "deferred") badges.push("Deferred");
  return [...new Set(badges)];
}

function validateState(state) {
  const errors = [];
  if (state.schemaVersion !== 2) errors.push("schemaVersion must be 2");
  if (state.currentUnderstanding) {
    if (!String(state.currentUnderstanding.body ?? "").trim()) errors.push("currentUnderstanding.body must be non-empty");
    if (!['Initial understanding', 'Current understanding'].includes(state.currentUnderstanding.label)) errors.push("currentUnderstanding.label is invalid");
    const source = state.currentUnderstanding.source ?? {};
    if (!['orientation', 'resolution', 'update'].includes(source.type)) errors.push("currentUnderstanding.source.type is invalid");
    for (const id of source.decisionIds ?? []) if (!findById(state.decisions, id)) errors.push(`currentUnderstanding references missing decision ${id}`);
    for (const id of source.evidenceIds ?? []) if (!findById(state.evidence, id)) errors.push(`currentUnderstanding references missing evidence ${id}`);
  }
  if (!Number.isInteger(state.revision) || state.revision < 0) errors.push("revision must be a non-negative integer");
  for (const name of COLLECTIONS) {
    if (!Array.isArray(state[name])) { errors.push(`${name} must be an array`); continue; }
    const ids = new Set();
    for (const item of state[name]) {
      if (!item?.id) errors.push(`${name} contains an item without id`);
      else if (ids.has(item.id)) errors.push(`${name} contains duplicate id ${item.id}`);
      ids.add(item?.id);
    }
  }
  const references = [
    ["tangents", "neighborhoodIds", "neighborhoods"],
    ["questions", "tangentIds", "tangents"],
    ["questions", "evidenceIds", "evidence"],
    ["proposals", "questionIds", "questions"],
    ["probes", "questionIds", "questions"],
    ["decisions", "questionIds", "questions"],
    ["decisions", "proposalIds", "proposals"],
    ["promotions", "decisionIds", "decisions"],
  ];
  for (const [source, field, target] of references) {
    const targetIds = new Set(state[target].map(({ id }) => id));
    for (const item of state[source]) for (const id of item[field] ?? []) if (!targetIds.has(id)) errors.push(`${source}.${item.id}.${field} references missing ${target} id ${id}`);
  }
  for (const tangent of state.tangents) if (!TANGENT_STATUSES.has(tangent.status ?? "tracked")) errors.push(`tangent ${tangent.id} has invalid status ${tangent.status}`);
  for (const question of state.questions) {
    if (!QUESTION_STATUSES.has(question.status ?? "open")) errors.push(`question ${question.id} has invalid status ${question.status}`);
    if (question.kind === "contradiction" && !(question.conflictsWith?.length >= 2)) errors.push(`contradiction ${question.id} lacks conflicting references`);
  }
  for (const review of state.reviews) {
    if (review.status !== "reviewing") errors.push(`review ${review.id} has invalid status ${review.status}`);
    for (const point of review.points ?? []) {
      if (point.purpose !== "informational" && !String(point.question ?? "").trim()) errors.push(`point ${point.id} lacks a user-facing question`);
      if (!String(point.context ?? "").trim()) errors.push(`point ${point.id} lacks explanatory context`);
      for (const id of point.questionIds ?? []) if (!findById(state.questions, id)) errors.push(`point ${point.id} references missing question ${id}`);
      for (const id of point.proposalIds ?? []) if (!findById(state.proposals, id)) errors.push(`point ${point.id} references missing proposal ${id}`);
    }
  }
  return errors;
}

function validateNewReference(collection, id, type) {
  const entity = findById(collection, id);
  if (!entity) throw new Error(`Missing ${type}: ${id}`);
  return entity;
}

function resolveMany(state, collection, refs) {
  return [...new Set((refs ?? []).map((ref) => resolveRef(state, collection, ref).id))];
}

function resolveRef(state, collection, ref) {
  if (typeof ref === "object" && ref?.id) ref = ref.id;
  requiredString(ref, `${collection} reference`);
  const prefix = PREFIXES[collection];
  const entity = state[collection].find((item) => item.id === ref || item.key === ref || item.id === `${prefix}-${slug(ref)}`);
  if (!entity) throw new Error(`Missing ${collection} reference: ${ref}`);
  return entity;
}

function allocateId(state, collection, value) {
  if (value.id) {
    requiredString(value.id, `${collection}.id`);
    if (findById(state[collection], value.id)) throw new Error(`Duplicate ${collection} id: ${value.id}`);
    return value.id;
  }
  const prefix = PREFIXES[collection];
  const basis = value.key ?? value.title ?? value.prompt ?? `${collection}-${state[collection].length + 1}`;
  const base = `${prefix}-${slug(basis)}`;
  let id = base;
  let suffix = 2;
  while (findById(state[collection], id)) id = `${base}-${suffix++}`;
  return id;
}

function allocateNestedId(state, collection, prefix, value) {
  if (value.id) {
    if (collection.some(({ id }) => id === value.id)) throw new Error(`Duplicate nested id: ${value.id}`);
    return value.id;
  }
  const basis = value.key ?? value.title ?? `${prefix}-${collection.length + 1}`;
  const base = `${prefix}-${slug(basis)}`;
  let id = base;
  let suffix = 2;
  const used = new Set([...collection.map(({ id }) => id), ...state.reviews.flatMap(({ points = [] }) => points.map(({ id }) => id))]);
  while (used.has(id)) id = `${base}-${suffix++}`;
  return id;
}

function inferQuestionPrompt(state, ids) {
  if (ids.length === 1) return requireEntity(state.questions, ids[0], "question").prompt;
  return ids.map((id) => requireEntity(state.questions, id, "question").prompt).join(" / ");
}

function findEntityAcrossState(state, id) {
  for (const collection of COLLECTIONS) {
    const entity = state[collection].find((item) => item.id === id || item.key === id);
    if (entity) return { collection, entity };
  }
  throw new Error(`Missing entity: ${id}`);
}

function createChangeSet() { return { added: [], updated: [], removed: [] }; }
function compactChanges(changed) {
  return {
    added: [...new Set(changed.added)],
    updated: [...new Set(changed.updated)],
    removed: [...new Set(changed.removed)],
  };
}
function requireEntity(collection, id, type) { return validateNewReference(collection, id, type); }
function findById(collection, id) { return collection.find((item) => item.id === id); }
function requiredString(value, name) { if (typeof value !== "string" || value.trim() === "") throw new Error(`${name} must be a non-empty string`); }
function slug(value) { return String(value).trim().toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 64) || "item"; }
function now() { return new Date().toISOString(); }
function isWithin(root, target) { return target === resolve(root) || target.startsWith(`${resolve(root)}/`); }
