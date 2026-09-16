# N03 P1 — kernel-backed descendant extinction

Implemented on `01947f9217aaa0e03287a0964ebef9ec07528219` in the isolated
`implement/legacy-plus-lifecycle` worktree by worker
`worker-b4c2ef8c85b5-73634ebe10` (attempt 1). This is ordinary implementation and
verification evidence, not project-model/DAG authority or release certification.
**N04 remains blocked until independent review of this substantive executor fix.**

## Defect and correction

Read `/tmp/n03-final-review-01947f9/review.md`, `adapter-churn.mjs`,
`churn-candidate.c`, `churn.mjs`, `churn.c`, and the review's subreaper wrapper.
The old non-atomic /proc negative scan was not an extinction proof: an ordinary
finite fork/exit handoff could outlive durable PASS/FAIL, advancement and cleanup.

- Replaced the Node session anchor and scan-based settlement with a packaged
  Python3 stdlib Linux `PR_SET_CHILD_SUBREAPER` supervisor. It adopts orphans and
  publishes extinction only after `os.waitpid(-1, WNOHANG)` raises ECHILD. This is
  kernel child accounting, not a repeated scan/quiet-period heuristic.
- Preserved the synced request/owner/workspace launch journal and current-intent
  lock. Ready is sent only after capability checks; actual argv launch requires
  a matching nonce gate after journal fsync and intent revalidation. Journal v2
  binds supervisor PID/boot/start identity and a fresh outcome nonce. Legacy v1
  journals confer no new extinction proof.
- The durable atomic/fsynced receipt binds nonce, supervisor identity and exact
  cwd. It records actual argv exit/signal separately from complete descendant
  reaping and interruption. Receipt lookup also follows supervisor exit (including
  an ENOENT/exit race), without depending on a terminal IPC acknowledgement.
  Protocol metadata survives clean candidate removal until lifecycle publication
  is durable; a crash in that gap cannot erase the only extinction receipt.
- Cancellation, expiry and owner pipe EOF initiate TERM/KILL inside the independent
  supervisor, surviving owner and command-leader exit. Positive /proc discovery
  is only for pidfd signaling, never for settlement. Missed forks remain waitable.
- Supervisor loss/missing/corrupt receipt after launch retains an unresolved job
  and workspace: no lifecycle result, blind respawn, retry or cleanup. A live
  supervisor without a receipt must finish. After supervisor death, an empty
  process table is not evidence: the existing explicit independent process/effect
  settlement callback remains mandatory. Recovery produces infrastructure BLOCKED
  and preserves known command exit/invocation from the receipt, not invented PASS.
- Requires Linux, readable same-namespace /proc, Python 3.9+ stdlib ctypes/pidfd
  APIs and kernel subreaper/pidfd capabilities. Unavailable capabilities fail
  closed before launch. The helper uses module-relative `import.meta.url` lookup
  and is covered by package.json's existing `extensions/` inclusion rule. No
  library, installation, package overlay or packed-artifact run was used.
- Trusted local command boundary is unchanged: no sandbox claim, hostile session
  escapes, same-UID receipt tampering or unrelated external effect containment.
  Producer callbacks retain their own protocols. Context/Git/F7 fixes are retained.

## Regression coverage

The actual `CommandRunnerV2`/`StoreV2`/`RuntimeV2` fixture runs a Python fork fixture
that checks the actual candidate's `baseline\n`, exits its direct command zero,
then hands off through 60 same-session forks, 20 ms apart, with disconnected
stdio, TERM resistance and a final gated sleeper. Normal and cancelled cases deny
durable result/PASS, advance, retry and workspace cleanup while descendants live;
normal release and cancelled KILL/reaping preserve the original command exit zero.

Crash tests cover owner death during churn, stopped-owner durable receipt
acknowledgement loss, and supervisor death without a receipt. The latter retains
ambiguity even after the known descendant stops. A separate test-only outer
subreaper provides genuine independent ECHILD settlement and collects test orphans;
it is not the production containment protocol. Tests also cover receipt nonce,
start identity, cwd and extinction mismatches, current-intent cancellation at the
actual subreaper launch gate, missing Python, real command exit/output bounds,
TERM-resistant timeout escalation, crash after candidate cleanup before lifecycle
publication, and existing context/Git/F7/state regressions.

## Final verification (fresh commands, no timeout classified as success)

Complete combined output: `<label>.log`. Exact inner command: `<label>.command`.
Actual `/usr/bin/time` exit and elapsed duration: `<label>.time`.
Each suite had its own **3600-second command timeout and 3700-second tool budget**;
the final three ran concurrently in independent disposable fixture repositories.
No aggregate wrapper shortened those individual budgets.

- `timeout 3600 node scripts/dag-v2-context-test.mjs` — **6/6**, exit **0**, **37.87 s**.
- `timeout 3600 node scripts/dag-v2-lifecycle-test.mjs` — **38/38**, exit **0**, **130.52 s**.
- `timeout 3600 node scripts/dag-v2-state-test.mjs` — **23/23**, exit **0**, **221.60 s**.

Final suite elapsed durations total **389.99 s** (not parallel wall time).
The timing wrapper was `/usr/bin/time -f 'exit=%x duration_seconds=%e' -o
<label>.time <inner-command> > <label>.log 2>&1`; commands, logs and time files
were first written under `/tmp/n03-subreaper-verification`, then copied here.
`verify.sh` reruns the same commands serially into a fresh evidence directory;
allow at least 10800 seconds aggregate plus harness overhead for that script.

Earlier successful development runs are retained, not substituted for final runs:
`lifecycle-first` (36/36, 108.72 s), `lifecycle-before-outer-reaper` (37/37,
179.47 s), `lifecycle-before-receipt-recovery` (37/37, 174.45 s),
`context-before-receipt-recovery` (6/6, 59.30 s), and
`state-before-receipt-recovery` (23/23, 285.87 s), all exit 0. The later
`*-before-publication-retention` batch also passed: context 6/6 in 34.04 s,
lifecycle 37/37 in 113.66 s, state 23/23 in 187.10 s. Final lifecycle includes
the independent test reaper, known-exit recovery, and the cleanup/publication
crash-window regression.
Python AST parsing and diff whitespace checks also passed (`checks.*`).

Environment: Linux 7.0.11, Node v24.19.0, existing
`/home/jordan/.nix-profile/bin/python3` (3.13.15). Existing repository dependencies
were temporarily exposed through a worker-created root node_modules symlink;
that symlink was removed before commit. No dependency installation or installed
package modification occurred. No historical run mutation, project-model authority
change, orchestration/delegation, external publication, push/tag or deployment.
No full release, package smoke, or broader Git integration suite was rerun; the
state suite does exercise real dependent Git landing/reload. Independent review
and N04/N05 work remain required.
