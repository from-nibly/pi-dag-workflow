import assert from "node:assert/strict";
import { assertPrototypeLayout, renderPrototype, SCENARIOS, VARIANTS, WIDTHS } from "./render.mjs";

let cases = 0;
for (const variant of VARIANTS) {
  for (const scenario of Object.keys(SCENARIOS)) {
    for (const width of WIDTHS) {
      const first = renderPrototype({ variant, scenario, width, frame: 0 });
      const repeat = renderPrototype({ variant, scenario, width, frame: 0 });
      assert.deepEqual(first, repeat, `${variant}/${scenario}/${width} must be deterministic`);
      assertPrototypeLayout(first, width);
      cases += 1;
    }
  }
}

const liveA = renderPrototype({ variant: "flow", scenario: "parallel", width: 80, frame: 0 });
const liveB = renderPrototype({ variant: "flow", scenario: "parallel", width: 80, frame: 1 });
assert.notEqual(liveA[1], liveB[1], "a freshly live worker must animate");
assert.equal(liveA[2], liveB[2], "a non-live lane must remain static");

const staleA = renderPrototype({ variant: "flow", scenario: "stale", width: 80, frame: 0 });
const staleB = renderPrototype({ variant: "flow", scenario: "stale", width: 80, frame: 9 });
assert.deepEqual(staleA, staleB, "stale liveness must freeze motion");

const narrow = renderPrototype({ variant: "flow", scenario: "parallel", width: 50, frame: 0 });
assert(narrow.some((line) => line.includes("F3 3/9")), "narrow layout must use numeric stage fallback");
const wide = renderPrototype({ variant: "flow", scenario: "parallel", width: 120, frame: 0 });
assert(wide.some((line) => line.includes("[■■■▶·····]")), "wide layout must retain nine stage cells");

console.log(`DAG widget activity-lane prototype OK: ${cases} deterministic responsive cases plus live/frozen motion checks.`);
