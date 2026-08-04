import { createHash } from "node:crypto";
import { assertTurnProjection, type ModelReviewTurnProjection, type PresentationBlock } from "./review-turn.ts";
import type { ReviewPoint } from "./types.ts";

export const RENDERER_CONTRACT_VERSION = 1;

export function renderReviewTurn(projection: ModelReviewTurnProjection): string {
  assertTurnProjection(projection);
  const digest = `sha256:${createHash("sha256").update(JSON.stringify(projection)).digest("hex")}`;
  const before = blocksAt(projection, "before-review");
  const after = blocksAt(projection, "after-review");
  const reviewPoints = projection.review.points.map((point) => renderPoint(projection, point)).join("\n");
  const frontier = (projection.frontier ?? []).map(renderFrontier).join("\n");
  const consequences = (projection.delta?.consequences ?? []).map((item) => `<li>${escapeHtml(item)}</li>`).join("");
  const config = safeJson({ reviewId: projection.review.id, reviewHash: projection.review.semanticHash });

  return `<!doctype html>
<html lang="en" data-lavish-live-reload-root>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="lavish-live-reload" content="root">
  <meta name="renderer-contract" content="${RENDERER_CONTRACT_VERSION}">
  <meta name="projection-digest" content="${digest}">
  <title>${escapeHtml(projection.review.title)} — ${escapeHtml(projection.project.title)}</title>
  <style>${SHELL_CSS}</style>
</head>
<body>
  <a class="skip" href="#review">Skip to decisions</a>
  <header class="topbar">
    <div>
      <span class="eyebrow">${escapeHtml(projection.project.title)} · revision ${projection.project.revision}</span>
      <h1>${escapeHtml(projection.review.title)}</h1>
      <p>${escapeHtml(projection.focus.title)} · <code>${escapeHtml(projection.review.id)}</code></p>
    </div>
    <div class="top-actions"><span class="status-chip">Active review</span></div>
  </header>
  <div class="layout">
    <aside aria-label="Turn navigation">
      <nav>
        <a href="#understanding">Current understanding</a>
        <a href="#delta">Model delta</a>
        <a href="#frontier">Frontier</a>
        <a href="#review">Review</a>
      </nav>
      <dl class="identity">
        <dt>Model</dt><dd class="hash">${escapeHtml(projection.project.modelHash)}</dd>
        <dt>Focus</dt><dd>${escapeHtml(projection.focus.id)}</dd>
        <dt>Projection</dt><dd>${escapeHtml(digest)}</dd>
      </dl>
    </aside>
    <main>
      <section class="hero card" id="understanding">
        <span class="section-label">Current understanding · non-authoritative synthesis</span>
        <div class="understanding-body markdown-body">${formatMarkdown(projection.currentUnderstanding.body)}</div>
      </section>
      <section id="delta" aria-labelledby="delta-heading">
        <div class="section-heading"><div><span class="section-label">Since the previous review</span><h2 id="delta-heading">Model delta</h2></div></div>
        <div class="stats">
          <article><strong>${number(projection.delta?.added)}</strong><span>Added</span></article>
          <article><strong>${number(projection.delta?.changed)}</strong><span>Changed</span></article>
          <article><strong>${number(projection.delta?.stillUnresolved)}</strong><span>Still unresolved</span></article>
        </div>
        ${projection.delta?.possibleMisunderstanding ? `<div class="callout warning"><strong>Possible misunderstanding</strong><p>${escapeHtml(projection.delta.possibleMisunderstanding)}</p></div>` : ""}
        ${consequences ? `<div class="card compact"><h3>Consequences</h3><ul>${consequences}</ul></div>` : ""}
      </section>
      <section id="frontier" aria-labelledby="frontier-heading">
        <div class="section-heading"><div><span class="section-label">Selected unresolved objects</span><h2 id="frontier-heading">Frontier</h2></div></div>
        <div class="frontier-grid">${frontier || '<p class="empty">No selected frontier objects.</p>'}</div>
        ${projection.frontierHandoff ? `<div class="handoff"><strong>What happens next</strong><p>${escapeHtml(projection.frontierHandoff)}</p></div>` : ""}
      </section>
      ${before}
      <section id="review" aria-labelledby="review-heading">
        <div class="section-heading"><div><span class="section-label">Hash-bound oversight turn</span><h2 id="review-heading">Review</h2></div><span class="hash-badge">${escapeHtml(projection.review.semanticHash)}</span></div>
        <div class="review-stack">${reviewPoints}</div>
      </section>
      ${after}
      <footer>
        <p>This artifact is disposable presentation state. The project model and explicit resolution receipts own semantics.</p>
        <button type="button" class="button" data-lavish-action id="send-feedback-bottom">Send queued feedback</button>
      </footer>
    </main>
  </div>
  <script>window.__MODEL_REVIEW__=${config};\n${SHELL_JS}</script>
</body>
</html>\n`;
}

function renderPoint(projection: ModelReviewTurnProjection, point: ReviewPoint) {
  const blocks = blocksAt(projection, `point:${point.id}`);
  const pointHash = semanticDigest(point);
  if (point.purpose === "awareness") {
    return `<article class="review-card awareness" id="point-${attr(point.id)}">
      <div class="point-head"><span class="purpose">For awareness</span><code>${escapeHtml(point.id)}</code></div>
      <h3>${escapeHtml(point.title)}</h3><div class="point-context markdown-body">${formatPointContext(point.context)}</div>${blocks}
      <button type="button" class="button secondary acknowledge" data-lavish-action data-point-id="${attr(point.id)}" data-point-hash="${attr(pointHash)}">Queue acknowledgement</button>
      <p class="form-status" aria-live="polite"></p>
    </article>`;
  }
  const options = point.options.map((option) => `<div class="option">
    <label class="option-choice"><input type="radio" name="choice-${attr(point.id)}" value="${attr(option.id)}" data-option-hash="${attr(option.semanticHash)}">
    <span><span class="option-title">${escapeHtml(option.label)}${option.recommended ? '<span class="recommended">Recommended</span>' : ""}</span>
    <span class="option-description">${escapeHtml(option.description)}</span>
    ${option.rationale ? `<span class="option-rationale"><strong>Why choose this:</strong> ${escapeHtml(option.rationale)}</span>` : ""}</span></label>
  </div>`).join("\n");
  const other = `<div class="option option-other"><label class="option-choice"><input type="radio" name="choice-${attr(point.id)}" value="__other__"><span><span class="option-title">Other</span><span class="option-description">Move away from all suggested options and provide alternate direction below.</span></span></label></div>`;
  return `<article class="review-card decision" id="point-${attr(point.id)}">
    <div class="point-head"><span class="purpose">Decision needed</span><code>${escapeHtml(point.id)}</code></div>
    <h3>${escapeHtml(point.title)}</h3><div class="point-context markdown-body">${formatPointContext(point.context)}</div>${blocks}
    <p class="question">${escapeHtml(point.question)}</p>
    <form class="decision-form" data-lavish-question="${attr(point.id)}" data-point-id="${attr(point.id)}" data-point-hash="${attr(pointHash)}">
      <fieldset><legend class="sr-only">${escapeHtml(point.question)}</legend>${options}${other}</fieldset>
      <label class="response-copy"><span>Optional context or modification</span><textarea name="responseText" rows="3" placeholder="Add context to a suggested option, or describe what should happen instead when choosing Other"></textarea></label>
      <div class="form-actions"><button type="submit" class="button primary">Queue this answer</button></div>
      <p class="form-status" aria-live="polite"></p>
    </form>
  </article>`;
}

function renderFrontier(item: ModelReviewTurnProjection["frontier"][number]) {
  const badges = (item.badges ?? []).map((badge) => `<span>${escapeHtml(badge)}</span>`).join("");
  return `<article class="frontier-card"><div class="point-head"><code>${escapeHtml(item.id)}</code><span class="state">${escapeHtml(item.state)}</span></div><h3>${escapeHtml(item.title)}</h3><p>${escapeHtml(item.summary ?? "")}</p><div class="badges">${badges}</div></article>`;
}

function blocksAt(projection: ModelReviewTurnProjection, placement: string) {
  return (projection.presentationBlocks ?? []).filter((block) => block.placement === placement).map(renderBlock).join("\n");
}

function renderBlock(block: PresentationBlock) {
  const title = block.title ? `<h3>${escapeHtml(block.title)}</h3>` : "";
  if (block.kind === "html") return `<div class="presentation-block trusted-html" data-block-id="${attr(block.id)}">${title}${block.html ?? ""}</div>`;
  if (block.kind === "markdown") return `<div class="presentation-block markdown markdown-body" data-block-id="${attr(block.id)}">${title}${formatMarkdown(block.body ?? "")}</div>`;
  if (block.kind === "callout") return `<div class="presentation-block callout ${attr(block.tone ?? "info")}" data-block-id="${attr(block.id)}">${title}<p>${escapeHtml(block.body ?? "")}</p></div>`;
  if (block.kind === "code") return `<div class="presentation-block" data-block-id="${attr(block.id)}">${title}<pre><code>${escapeHtml(block.code ?? "")}</code></pre></div>`;
  if (block.kind === "table") return `<div class="presentation-block" data-block-id="${attr(block.id)}">${title}<div class="table-wrap"><table><thead><tr>${(block.headers ?? []).map((header) => `<th>${escapeHtml(header)}</th>`).join("")}</tr></thead><tbody>${(block.rows ?? []).map((row) => `<tr>${row.map((cell) => `<td>${escapeHtml(cell)}</td>`).join("")}</tr>`).join("")}</tbody></table></div></div>`;
  if (block.kind === "comparison") return `<div class="presentation-block" data-block-id="${attr(block.id)}">${title}${block.intro ? `<p>${escapeHtml(block.intro)}</p>` : ""}<div class="comparison">${(block.columns ?? []).map((column) => `<article><h4>${escapeHtml(column.title)}</h4><ul>${(column.items ?? []).map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul></article>`).join("")}</div></div>`;
  return "";
}

function semanticDigest(value: unknown) { return `sha256:${createHash("sha256").update(JSON.stringify(value) ?? "null").digest("hex")}`; }

function formatPointContext(value: unknown) {
  const headings: Record<string, string> = {
    "WHY THIS DECISION EXISTS": "Why this decision exists",
    "EXAMPLE": "Example",
    "RECOMMENDATION": "Recommendation",
    "WHAT THIS DOES NOT DO": "What this does not do",
    "IMPORTANT BOUNDARY": "Important boundary",
    "COST": "Cost",
  };
  const markdown = String(value ?? "").replace(
    /(^|\s)(WHY THIS DECISION EXISTS|EXAMPLE|RECOMMENDATION|WHAT THIS DOES NOT DO|IMPORTANT BOUNDARY|COST)\s+—\s+/g,
    (_match, _prefix, label: string) => `\n\n### ${headings[label]}\n\n`,
  ).trim();
  return formatMarkdown(markdown);
}

function formatMarkdown(value: unknown) {
  const output: string[] = [];
  const paragraph: string[] = [];
  let list: { tag: "ul" | "ol"; items: string[] } | undefined;
  const flushParagraph = () => {
    if (!paragraph.length) return;
    output.push(`<p>${paragraph.map(formatInlineMarkdown).join("<br>")}</p>`);
    paragraph.length = 0;
  };
  const flushList = () => {
    if (!list) return;
    output.push(`<${list.tag}>${list.items.map((item) => `<li>${formatInlineMarkdown(item)}</li>`).join("")}</${list.tag}>`);
    list = undefined;
  };
  for (const line of String(value ?? "").replace(/\r\n?/g, "\n").split("\n")) {
    if (!line.trim()) { flushParagraph(); flushList(); continue; }
    const heading = /^(#{1,4})\s+(.+)$/.exec(line);
    if (heading) {
      flushParagraph(); flushList();
      const level = Math.min(6, heading[1].length + 2);
      output.push(`<h${level}>${formatInlineMarkdown(heading[2])}</h${level}>`);
      continue;
    }
    const unordered = /^\s*[-*]\s+(.+)$/.exec(line);
    const ordered = /^\s*\d+[.)]\s+(.+)$/.exec(line);
    if (unordered || ordered) {
      flushParagraph();
      const tag = unordered ? "ul" : "ol";
      if (list?.tag !== tag) { flushList(); list = { tag, items: [] }; }
      list!.items.push((unordered ?? ordered)![1]);
      continue;
    }
    flushList();
    paragraph.push(line);
  }
  flushParagraph(); flushList();
  return output.join("");
}
function formatInlineMarkdown(value: string) {
  return value.split(/(`[^`\n]+`)/g).map((part) => {
    if (part.startsWith("`") && part.endsWith("`")) return `<code>${escapeHtml(part.slice(1, -1))}</code>`;
    return escapeHtml(part)
      .replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>")
      .replace(/__([^_]+)__/g, "<strong>$1</strong>")
      .replace(/(^|[^*])\*([^*]+)\*(?!\*)/g, "$1<em>$2</em>");
  }).join("");
}
function escapeHtml(value: unknown) {
  const entities: Record<string, string> = { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" };
  return String(value ?? "").replace(/[&<>"']/g, (char) => entities[char] ?? char);
}
function attr(value: unknown) { return escapeHtml(String(value ?? "").replace(/[^A-Za-z0-9._:-]/g, "-")); }
function safeJson(value: unknown) { return (JSON.stringify(value) ?? "null").replaceAll("<", "\\u003c").replaceAll(" ", "\\u2028").replaceAll(" ", "\\u2029"); }
function number(value: unknown) { return typeof value === "number" && Number.isFinite(value) ? value : 0; }

const SHELL_JS = String.raw`
(() => {
  const review = window.__MODEL_REVIEW__;
  const lavish = () => window.lavish && typeof window.lavish.queuePrompt === "function" ? window.lavish : null;
  function setStatus(element, text, isError = false) {
    const status = element.querySelector(".form-status");
    if (!status) return;
    status.textContent = text;
    status.classList.toggle("error", isError);
  }
  function queue(prompt, data, element, text) {
    const api = lavish();
    if (!api) { setStatus(element, "Lavish is unavailable; open this artifact through Lavish Editor.", true); return false; }
    api.queuePrompt(prompt, { tag: "model-review", text, element, queueKey: data.pointId, data });
    setStatus(element, "Queued. Review other points, then send all queued feedback.");
    return true;
  }
  document.querySelectorAll(".decision-form").forEach((form) => form.addEventListener("submit", (event) => {
    event.preventDefault();
    const selected = form.querySelector("input[type=radio]:checked");
    const responseText = form.elements.responseText.value.trim();
    if (!selected) { setStatus(form, "Choose a suggested option or Other.", true); return; }
    const pointId = form.dataset.pointId;
    const optionId = selected.value;
    if (optionId === "__other__" && !responseText) { setStatus(form, "Describe the alternate direction.", true); return; }
    const optionLabel = selected.closest(".option").querySelector(".option-title").childNodes[0].textContent.trim();
    const action = optionId === "__other__" ? "modify" : "accept";
    const data = { reviewId: review.reviewId, reviewHash: review.reviewHash, pointId, pointHash: form.dataset.pointHash, action, ...(action === "accept" ? { optionId, optionHash: selected.dataset.optionHash } : {}), ...(responseText ? { responseText } : {}) };
    const prompt = action === "accept" ? "Review response for " + review.reviewId + "/" + pointId + ": select " + optionId + (responseText ? ". Additional context: " + responseText : "") : "Alternate direction for " + review.reviewId + "/" + pointId + ": " + responseText;
    queue(prompt, data, form, action === "accept" ? "Select " + optionLabel : "Other direction");
  }));
  document.querySelectorAll(".acknowledge").forEach((button) => button.addEventListener("click", () => {
    const card = button.closest(".review-card");
    const pointId = button.dataset.pointId;
    queue("Awareness acknowledged for " + review.reviewId + "/" + pointId, { reviewId: review.reviewId, reviewHash: review.reviewHash, pointId, pointHash: button.dataset.pointHash, action: "awareness" }, card, "Awareness acknowledged");
  }));
  function send() {
    const api = lavish();
    if (!api || typeof api.sendQueuedPrompts !== "function") return;
    api.sendQueuedPrompts();
  }
  document.getElementById("send-feedback-bottom").addEventListener("click", send);
})();`;

const SHELL_CSS = String.raw`
:root{color-scheme:light;--ink:#18201d;--muted:#5e6a64;--paper:#f5f2e9;--panel:#fffdf7;--line:#d8d3c6;--green:#195c4a;--green2:#d8eee5;--gold:#a96614;--gold2:#f7e9c9;--red:#a23d36;--shadow:0 12px 36px rgba(30,40,36,.09);font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:var(--paper);color:var(--ink);line-height:1.55}button,input,textarea{font:inherit}button{cursor:pointer}.skip{position:absolute;width:1px;height:1px;overflow:hidden;clip-path:inset(50%);white-space:nowrap}.skip:focus{position:fixed;left:1rem;top:1rem;width:auto;height:auto;overflow:visible;clip-path:none;background:var(--ink);color:white;padding:.7rem 1rem;z-index:20}.topbar{display:flex;justify-content:space-between;gap:2rem;align-items:flex-end;padding:2rem clamp(1rem,4vw,4rem);background:var(--ink);color:white}.topbar>div{min-width:0}.topbar h1{font-family:Georgia,serif;font-size:clamp(2rem,5vw,4.5rem);line-height:1;margin:.35rem 0}.topbar p{margin:0;color:#cbd4cf;overflow-wrap:anywhere}.topbar code{overflow-wrap:anywhere;word-break:break-word}.top-actions{display:flex;align-items:center;gap:.8rem;flex-wrap:wrap}.eyebrow,.section-label{text-transform:uppercase;letter-spacing:.13em;font-size:.73rem;font-weight:800}.status-chip{background:#d6f3e5;color:#124a3b;border-radius:999px;padding:.35rem .65rem;font-size:.78rem;font-weight:800}.layout{display:grid;grid-template-columns:17rem minmax(0,1fr);width:100%;max-width:92rem;margin:auto}aside{padding:2rem 1.25rem;position:sticky;top:0;height:100vh;border-right:1px solid var(--line)}nav{display:grid;gap:.25rem}nav a{color:var(--ink);text-decoration:none;padding:.65rem .75rem;border-radius:.5rem}nav a:hover,nav a:focus{background:var(--green2)}.identity{font-size:.78rem;margin-top:2rem}.identity dt{font-weight:800;margin-top:.8rem}.identity dd{margin:.15rem 0;color:var(--muted);overflow-wrap:anywhere}.hash{word-break:break-all}main{min-width:0;padding:2.5rem clamp(1rem,4vw,4rem) 5rem;display:grid;gap:3.5rem}.card,.presentation-block{background:var(--panel);border:1px solid var(--line);border-radius:1rem;padding:1.4rem;box-shadow:var(--shadow)}.card.compact{box-shadow:none;margin-top:1rem}.hero{border-top:5px solid var(--green)}.understanding-body{max-width:70rem}.markdown-body h3,.markdown-body h4,.markdown-body h5,.markdown-body h6{font-family:Georgia,serif;line-height:1.2;margin:1.1rem 0 .45rem}.markdown-body h3:first-child,.markdown-body h4:first-child{margin-top:.55rem}.markdown-body p{margin:.65rem 0}.markdown-body ul,.markdown-body ol{margin:.65rem 0;padding-left:1.5rem}.markdown-body li+li{margin-top:.28rem}.markdown-body code{background:#ece8dd;border-radius:.25rem;padding:.08rem .3rem;font-size:.9em}.section-heading{display:flex;justify-content:space-between;gap:1rem;align-items:end;margin-bottom:1rem}.section-heading h2{font-family:Georgia,serif;font-size:2.2rem;margin:.2rem 0}.stats{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:1rem}.stats article{background:var(--panel);border:1px solid var(--line);border-radius:.8rem;padding:1rem}.stats strong{font-size:2rem;display:block}.stats span{color:var(--muted)}.callout{border-left:5px solid var(--gold);background:var(--gold2);padding:1rem 1.2rem;margin-top:1rem;border-radius:.5rem}.prototype-notice{margin-top:0;border-color:var(--green);background:var(--green2)}.callout p{margin:.3rem 0}.frontier-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,18rem),1fr));gap:1rem}.frontier-card{background:var(--panel);border:1px solid var(--line);border-radius:.8rem;padding:1rem;min-width:0}.frontier-card h3{margin:.5rem 0}.point-head{display:flex;justify-content:space-between;gap:1rem;align-items:center;min-width:0;color:var(--muted);font-size:.78rem}.point-head code{min-width:0;overflow-wrap:anywhere;word-break:break-word}.state,.purpose,.badges span,.recommended,.hash-badge{display:inline-block;border-radius:999px;padding:.25rem .55rem;background:var(--green2);color:var(--green);font-size:.72rem;font-weight:800}.badges{display:flex;flex-wrap:wrap;gap:.35rem}.badges span{background:#ece8dd;color:var(--muted)}.hash-badge{max-width:20rem;overflow-wrap:anywhere}.review-stack{display:grid;gap:1.3rem}.review-card{background:var(--panel);border:1px solid var(--line);border-radius:1rem;padding:1.4rem;box-shadow:var(--shadow);min-width:0}.review-card.awareness{border-left:5px solid var(--green)}.review-card.decision{border-left:5px solid var(--gold)}.review-card h3{font-family:Georgia,serif;font-size:1.55rem;margin:.7rem 0}.question{font-weight:800;font-size:1.1rem;margin-top:1.3rem}.decision-form fieldset{border:0;padding:0;display:grid;gap:.7rem}.option{padding:1rem;border:1px solid var(--line);border-radius:.8rem;background:#fff;min-width:0}.option-choice{display:grid;grid-template-columns:auto minmax(0,1fr);gap:.8rem;cursor:pointer}.option:has(input:checked){border-color:var(--green);outline:2px solid rgba(25,92,74,.18)}.option input{margin-top:.3rem}.option-title{display:flex;gap:.5rem;align-items:center;font-weight:800;flex-wrap:wrap}.option-description,.option-rationale{display:block;color:var(--muted);margin-top:.2rem}.option-rationale{font-style:italic;font-size:.9rem}details{margin-top:.6rem}summary{cursor:pointer;color:var(--green);font-size:.86rem}pre{overflow:auto;background:#171d1a;color:#edf3ef;padding:1rem;border-radius:.6rem;font-size:.78rem;white-space:pre-wrap;overflow-wrap:anywhere}.response-copy{display:grid;gap:.4rem;margin-top:1rem;font-weight:700}.response-copy textarea{width:100%;border:1px solid var(--line);border-radius:.6rem;padding:.7rem;resize:vertical;background:white}.handoff{margin-top:1rem;padding:1rem 1.2rem;border-left:5px solid var(--green);border-radius:.5rem;background:var(--green2)}.handoff p{margin:.3rem 0 0}.form-actions{display:flex;justify-content:flex-end;margin-top:.8rem}.form-status{min-height:1.2rem;color:var(--green);font-size:.85rem}.form-status.error{color:var(--red)}.button{border:1px solid var(--ink);background:transparent;color:inherit;border-radius:.55rem;padding:.62rem .9rem;font-weight:800}.button.primary{background:var(--green);color:white;border-color:var(--green)}.button.secondary{border-color:var(--green);color:var(--green)}.presentation-block{margin:1rem 0;box-shadow:none}.comparison{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,16rem),1fr));gap:1rem}.comparison article{background:#f7f5ee;border-radius:.7rem;padding:1rem}.table-wrap{overflow:auto}table{border-collapse:collapse;width:100%}th,td{text-align:left;border-bottom:1px solid var(--line);padding:.65rem;overflow-wrap:anywhere}.rich-demo{padding:1rem;background:var(--green2);border-radius:.7rem}.rich-demo button{margin:.4rem;border:1px solid var(--green);border-radius:.4rem;background:white;padding:.4rem .6rem}footer{border-top:1px solid var(--line);padding-top:1.5rem;display:flex;justify-content:space-between;gap:1rem;align-items:center;color:var(--muted)}.empty{color:var(--muted)}.sr-only{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border:0}@media(max-width:800px){.topbar{align-items:flex-start;flex-direction:column}.layout{grid-template-columns:1fr}aside{position:static;height:auto;border-right:0;border-bottom:1px solid var(--line)}nav{grid-template-columns:repeat(2,minmax(0,1fr))}.stats{grid-template-columns:1fr}.section-heading,footer{align-items:flex-start;flex-direction:column}.hash-badge{max-width:100%}}@media(prefers-reduced-motion:reduce){html{scroll-behavior:auto}}
`;
