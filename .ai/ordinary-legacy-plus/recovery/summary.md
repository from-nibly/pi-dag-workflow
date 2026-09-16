# Ordinary V1 recovery slice — completed

Owner: worker-e430ffd3db20-1ce78ba395, attempt 1.
Worktree: `/home/jordan/gh/from-nibly/pi-dag-workflow/.git/ordinary-legacy-plus-recovery`
Branch: `implement/legacy-plus-recovery`; base: `e5728373d7b98d1493547b703d84c9cc5c50df05`.
Read the complete ordinary implementation plan before work. No DAG/project-model orchestration, historical run operations, installed-package edits, package installation, publication, or external effects were performed. Tests use disposable local repositories. The sole dependency setup was the authorized local `node_modules` symlink (untracked).

## Review and commits

Reviewed all three supplied diffs before cherry-picking. No blocking findings in their bounded V1 recovery behavior:

- `5211439265c075e5f606599a4dde25ab5bc0a8f5` -> `ee2ec2d`: empty-ledger retries are bound to the known policy, exact failed owned-worker evidence, reconciled cleanup/effects and conservative two-replacement ceiling. Failed evidence/candidate bindings and reducer CAS remain intact.
- `5f7139fea80aa747a7f68d5d0a9059f5adbf8ad7` -> `b3eb533`: text exposes selectors that registered semantic tools actually require; tests consume displayed selectors rather than hidden details.
- `c1b61a8de20ed5211f53f67e67b3c47bcbe770b8` -> `8e1c13a`: integration request context hydrates only the exact immutable repository-binding fact; missing/corrupt facts, wrong selectors/session and non-PASS validation still fail closed.

Confirmed separate integration bug: `rederiveCurrent` treated a running run with only pending successors as initializing; scheduler, reservation reducer and semantic discovery then disallowed new admission. Existing dogfood genesis marked every node ready and had no causal edges, masking this boundary.

Fix and regressions: `153f7f6f0afc37db4fbebd9d098f0bbed35f5363` (`fix(dag): admit pending successors after integration`). Running pending items keep run projection active without fabricating ready state. Scheduler, exact reservation reducer and semantic discovery also admit retained initializing V1 snapshots through existing guards. No V2 runtime implemented. No migration or discovery-time persistence was added.

Tests now create two causally dependent nodes, keep successor pending through actual predecessor Git landing, reload persisted start context into a fresh store/service, execute the successor's semantic F0 action, and finish both real Git landings. A separate disposable fixture emulates the old already-stranded initializing projection while preserving actual integration evidence/revision/predecessor chain. Both exercise read-only discovery, stale revision/hash/epoch rejection, stale action rejection, authority/dependency/freshness/effect guards, and unchanged predecessor evidence. Fixture timestamps now follow persisted snapshot time after semantic continuation instead of moving backward to the fixed genesis timestamp.

## Verification ledger

All paths below are relative to this directory. Exact shell command format used (Bash):

`/usr/bin/time -f 'exit=%x duration_seconds=%e' -o .ai/ordinary-legacy-plus/recovery/NAME.time timeout BUDGET node SCRIPT [ARGS] > .ai/ordinary-legacy-plus/recovery/NAME.log 2>&1`

- NAME=`planning-runtime`, BUDGET=`3600`, SCRIPT=`scripts/dag-planning-runtime-test.mjs`: exit 0, 146.12s, 8 tests including bounded empty-ledger retries and registered-tool selectors. Logs: `planning-runtime.log`, `planning-runtime.time`.
- NAME=`runtime`, BUDGET=`3600`, SCRIPT=`scripts/dag-runtime-test.mjs`: exit 0, 238.42s. Logs: `runtime.log`, `runtime.time`.
- NAME=`git-integration`, BUDGET=`3600`, SCRIPT=`scripts/git-integration-test.mjs`: exit 0, 101.99s, actual-Git transaction/failpoint matrix. Logs: `git-integration.log`, `git-integration.time`.
- NAME=`dogfood`, BUDGET=`7200`, SCRIPT=`scripts/dag-dogfood-test.mjs`: exit 0, 2654.65s, all 21 scenarios including original hotfix regressions and both new dependent-node scenarios. Logs: `dogfood.log`, `dogfood.time`.
- NAME=`successors`, BUDGET=`3600`, SCRIPT=`scripts/dag-dogfood-test.mjs`, ARGS=`--scenario successorIntegrationRecovery,strandedSuccessorRecovery`: exit 0, 788.12s. Logs: `successors.log`, `successors.time`.
- NAME=`successors-final`, same budget/script/args: exit 0, 693.14s. Final focused rerun after replacing the negative probe's unsupported item-desired `hold` value with the valid `pause` value; production code unchanged since full suites. Logs: `successors-final.log`, `successors-final.time`.
- Exact command: `/usr/bin/time -f 'exit=%x duration_seconds=%e' -o .ai/ordinary-legacy-plus/recovery/diff-check.time git diff --check > .ai/ordinary-legacy-plus/recovery/diff-check.log 2>&1`: exit 0, 0.00s.

Initial test-development failures are retained, not represented as passes. All used the same 3600-second successor command above:

- `successors-fixture-error`: exit 1, 2.08s; causal edge duplicated the default semantic mutex. Disabled that redundant fixture mutex.
- `successors-probe-error`: exit 1, 142.16s; negative scheduler probe needed a rehashed snapshot.
- `successors-clock-error`: exit 1, 174.07s; semantic continuation advanced time beyond fixture's fixed AT. Updated fixture commands/observations to persisted snapshot time.
- `successors-seal-error`: exit 1, 598.67s; stranded snapshot sealing incorrectly included the old snapshotHash. Removed it before sealing.

## Handoff and remaining scope

Review and cherry-pick the four implementation commits above in order. Evidence files are committed separately. Parent owns integration with new V2 writers and final certification. Portfolio, full uncached release readiness and extracted-package smoke were not run for this bounded slice; passing focused suites is not a final release gate. Shared conductor/reducer surfaces will need deliberate reconciliation with the parent's V2 work. Historical V1 read-only policy for new product entrypoints remains parent-owned; these changes preserve the existing explicitly invoked V1 recovery service only. The historical real run and installed overlay were left untouched.
