# N03 — direct lifecycle evidence

Status: implemented and locally verified; ready for owner review/integration and serialized N04 work. This is ordinary coding evidence, not a canonical DAG stage record or release certification.

- Worker: `worker-d1699c1e61bf-7e59d7b526`, attempt 1.
- Worktree: `/home/jordan/gh/from-nibly/pi-dag-workflow/.git/ordinary-legacy-plus-lifecycle`.
- Branch: `implement/legacy-plus-lifecycle`.
- Reviewed/integrated N02 base: `1af5c090dd17d1fe0a5e6fc029d0759d86b6af98`.
- Implementation commit: `030760e7f81d5c446eb6bc070848c0095a13c43a` (`feat(runtime-v2): enforce direct lifecycle execution evidence`).
- Evidence commit: `e141b57` retains this ledger, exact commands, complete stdout/stderr logs, exits and elapsed durations. A following documentation-only correction removes trailing command separators from metadata and its formatter; raw test logs are unchanged. No integration into main was performed by this worker.

## Delivered

- Closed V2 plan lifecycle definitions: grounded oracle/check references resolve to hash-bound source refs, fixed applicable F1–F7 checks, command argv or trusted-producer identity, replay class and environment. Review-only check strings are not evidence. Unsupported/missing applicability cannot become PASS.
- Service-computed F0 before generic implementation dispatch. Exact native candidate binding is required before verification. Initial authoring is not counted as repair.
- Durable per-check intent/result operations tied to whole-plan selector, run, item, bound worker attempt, generation, stage attempt/round, candidate commit/tree, procedure/environment and authorized local scope.
- Concrete argv-only command runner and invoked trusted producers in newly materialized exact-candidate Git worktrees. Retained exit/signal, individual disposition, bounded stdout/stderr, concrete observations/findings, timing, measured runtime, candidate and actionable failing-command diagnostics. Unused/missing producers are never attested as executed.
- Fresh F2/F5 contexts, including cross-item reuse rejection; F7 replays every applicable check on clean exact final candidates; F8 is computed internally. Even a permissive Git adapter plus a generic worker completion claim cannot bypass `RuntimeV2.integrate`'s lifecycle guard.
- Typed findings and whole-run replan holds; explicit finding dispositions; typed F1/F3/F5/infrastructure back-edges; separate finite retry dimensions and retained no-progress/candidate history. Replacement/cancellation never reset budgets or authorize old evidence.
- Durable executor intent before invocation, generation recheck under the actual launch lock, result-before-ack publication, exact replay, stale-result quarantine, confirmed non-execution reconciliation and dead-executor settlement reconciliation. Unknown outcomes remain BLOCKED, never invented PASS or blind respawn.
- Existing V2 integration fixtures now run real lifecycle commands (including actual dependent-node Git landing/reload). Generic worker manager interfaces stay DAG-agnostic. No historical V1 writer or test was edited.

## Final verification

Commands are retained individually in `<label>.command`; complete combined stdout/stderr is in `<label>.log`; `/usr/bin/time` exit and elapsed seconds are in `<label>.time`. `verify.sh` reproduces both batches. Each final code suite exited 0; the two V2 suites each passed 23/23 tests.

- `node scripts/dag-v2-lifecycle-test.mjs` — exit 0, **58.08 s** (`lifecycle-final.*`).
- `node scripts/dag-v2-state-test.mjs` — exit 0, **148.69 s** (`state-final.*`).
- `node scripts/project-model-test.mjs` — exit 0, **10.88 s** (`model.*`).
- `node scripts/dag-planning-test.mjs` — exit 0, **2.67 s** (`planning.*`).
- `node scripts/dag-runtime-test.mjs` — exit 0, **236.64 s** (`runtime.*`).
- `node scripts/worker-runtime-test.mjs` — exit 0, **44.41 s** (`workers.*`).
- `node scripts/git-integration-test.mjs` — exit 0, **93.97 s** (`git.*`).
- `node scripts/dag-planning-runtime-test.mjs` — exit 0, **126.60 s** (`planning-runtime.*`).
- `git diff 1af5c09 --check` — exit 0 across the full deliverable (`diff-final.*`); staged whitespace validation also passed before implementation commit. Evidence staging later exposed trailing spaces in generated `.command` metadata; the metadata/formatter were corrected and the full-base check rerun successfully.

Final suite durations sum to **721.94 s** (not parallel wall time). Tool budgets were **7200 seconds** for the focused aggregate and **14400 seconds** for the regression aggregate; individual exploratory runs had 3600-second budgets. No timeout was classified as PASS.

Environment: Linux/local filesystem/flock, Node `v24.19.0`; existing root `node_modules` was temporarily symlinked into this worktree for tests, then that worker-created symlink was removed. No dependency installation or installed-package mutation occurred.

## Review/failure ledger

This worker performed a local code/invariant review; no independent reviewer was delegated and no canonical F2/F5 claim is made for this coding task.

Retained initial failures are honest intermediate development results, superseded by the final logs:

- `state-initial`: executor PID incorrectly used the bounded logical-count schema; changed to the existing process PID range.
- `lifecycle-initial`: test-file syntax error, corrected.
- `lifecycle-02`: unavailable command disposition was incorrectly overwritten, plus a cancellation test raced a short store lock and removed its fixture before pending execution settled. Corrected unavailable-command semantics, lock-only result-publication retries and test settlement handling.
- `lifecycle-03`: auditing previously passed stages incorrectly rejected newly observed blocking findings, and a test compared canonical null-prototype records against ordinary objects. Moved finding blocking to advancement/readiness, and compared normalized fixture values.
- `lifecycle-04` and `state-02`: earlier passing suites; final suites additionally cover review-driven hardening.

Review-driven hardening included pre-dispatch F0, actual invocation-time generation fencing, durable interrupted-job workspace identity, immediate repeated-tree adoption stops, fresh-context checks across items, and a real invalid behavioral candidate that passes static checking but fails the independent oracle. All are exercised by the final suites.

## N04/N05 handoff and explicit limits

- **N04 remains responsible** for native Git integration identity, exact composition, target CAS, isolated combined-state checks, repository locks, landing/effect reconciliation and restricted-effect authority. `IntegrationsV2.verify` is still mandatory, but lifecycle readiness is now checked internally before it. Exported retry dimensions/invalidation helpers are available for N04; do not weaken the readiness predicate.
- **N05 remains responsible** for product tools/commands/widgets, independently hydrated governing-source freshness, durable generic worker-manager adapters/completion and settlement authentication, end-to-end product recovery, full uncached release certification and extracted-package smoke. No full release or packed-artifact readiness claim is made here.
- `ResultsV2` and `CandidateInspectorV2` are explicit local trusted boundaries, with a working concrete `CommandRunnerV2`. Worker-backed F2/F5 producers must hydrate their actual fresh context/lineage; invocation of a JS callback alone does not attest a fresh worker. Generic worker status by itself never satisfies lifecycle checks.
- The minimal supported lifecycle profile requires applicable checks at every checkpoint, supports required/positively grounded not-applicable checks, and replays all applicable checks at F7. Conditional-expression applicability, waivers and non-replayable validity windows are intentionally unsupported/fail-closed, not silently waived. Candidate changes conservatively revalidate from F1 rather than trusting an unimplemented evidence-only-delta procedure.
- Local trusted commands must honor no-edit and repository-local effect contracts. This is not an OS sandbox and does not grant publication authority, install dependencies, or provide credentials. No ordinary executable-byte consent/attestation layer was added.
- Interrupted execution without a durable outcome requires independently verified process-tree/effect settlement. The reconciliation callback receives the durable owner/workspace job; parent death alone does not prove child settlement. A still-live host with an unresolved job is not automatically considered dead. Dirty/ambiguous check worktrees are retained rather than force-cleaned.
- New lifecycle fields are required in V2 plans; incomplete experimental N02 V2 snapshots fail closed, with no repair-on-read or implicit migration. Historical V1 paths, bytes, records, writers and original tests remain untouched.

No canonical DAG tools, historical run mutations, project-model authority changes, delegation, package installation, publication, push, tag or external deployment were performed.
