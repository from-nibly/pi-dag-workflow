# N04 — blocked on safe landing capability

Owner: worker-11719011f79f-1639a39a6f, attempt 1.
Worktree: `/home/jordan/gh/from-nibly/pi-dag-workflow/.git/ordinary-legacy-plus-git`.
Base: `45d24e4` (integrated reviewed N03).

## Status

**N04 is NOT implemented or certified.** No native V2 operation schema, durable
repository binding, combined-validation driver, landing implementation, or
cancellation/effect reconciliation has been added. N03 production code and V1
behavior are unchanged. N05 must not treat the existing trusted integration
callback as a safe concrete driver. README records this explicitly.

This attempt stopped rather than introducing a precheck-plus-merge driver that
would violate the requested expected-old/worktree safety requirement. The new
`scripts/dag-v2-git-test.mjs` is explicitly a characterization suite, not the
requested complete N04 acceptance suite. Its passing assertions demonstrate an
unsafe primitive, not a shipped protection.

## Actual blocking evidence

Four disposable-repository tests run **real Git 2.54.0**:

1. Precreate a fixture-owned target ref lock, then run ordinary fast-forward.
   Git exits 128 with target still old but index and files at the proposal.
2. Check expected-old/clean state, then inject a backward third target with the
   same tree before merge starts. Git exits 0 and lands from the unapproved third
   target: caller precheck is not expected-old CAS.
3. Pause the real merge at a fixture-only post-index-change hook, move the ref
   from expected old to third, then release it. Git correctly rejects the ref CAS
   (exit 128), preserves the third ref, but leaves index/files at the proposal.
4. Reject the prepared branch update from a fixture-only reference-transaction
   hook. Git exits 128 and preserves old ref, again after proposal index/files
   have been installed. A reference hook alone therefore cannot fix the window.

The test scripts disable inherited Git configuration selectors, global/system
config, hooks except the explicit fixture gates, prompts, replacements and lazy
fetch. All intentional ref drift and deletion occur solely in newly created
throwaway repositories. No project branch, historical run, model authority,
installed overlay or external system was changed. The temporary node_modules
symlink was removed after testing.

The common-dir cooperative lock is necessary but does not serialize arbitrary
Git writers. Durable intent and old/new/third reconciliation can describe these
outcomes but cannot retroactively prevent partial worktree effects. Post-failure
reset/stash/force cleanup would be unsafe and is not implemented.

The governing ordinary-fast-forward contract explicitly excludes raw checked-
branch update-ref. A prepared-ref-transaction plus two-tree checkout design would
be a different landing protocol, with new partial-checkout, lock-owner death,
HEAD/branch-switch, cancellation and cleanup obligations; this attempt does not
claim such a protocol is impossible, implemented, or safe. Alternatively an
independently enforceable exclusive-worktree capability could narrow the threat
model, but a cooperative flock is not that capability. Resolve that boundary
before implementing the rest of N04 and before N05 product wiring.

## Verification

Exact commands, actual exits, wall durations and log paths are in `results.jsonl`.
Every suite received a 3600-second individual timeout; the five-suite regression
batch received a 20000-second aggregate tool budget. No timeouts occurred.

- Initial 3-case characterization: exit 0, 1.178 seconds (`git-capability.log`).
- V2 context: exit 0, 44.367 seconds (`dag-v2-context-test.log`).
- V2 lifecycle: exit 0, 150.035 seconds (`dag-v2-lifecycle-test.log`).
- V2 state: exit 0, 247.049 seconds (`dag-v2-state-test.log`).
- Existing Git integration: exit 0, 126.097 seconds (`git-integration-test.log`).
- Existing runtime: exit 0, 304.912 seconds (`dag-runtime-test.log`).
- First 4-case characterization: exit 1, 1.383 seconds
  (`git-capability-final.log`): assertion expected an older Git error string;
  actual message was `update aborted by the reference-transaction hook`.
  Fixed the diagnostic regex without weakening ref/index/file assertions.
- Four-case characterization after regex repair: exit 0, 1.326 seconds
  (`git-capability-final-retry.log`).
- Final four-case characterization with Bash fixture hooks and quoted path
  transport: exit 0, 1.226 seconds (`git-capability-committed.log`).
- Final `git diff --check`: exit 0, 0.008 seconds (`diff-check-final.log`).

The five existing suites ran before the final documentation and fourth
characterization assertion were added; no production implementation changed
before or after those runs. They establish no N04 transactional guarantee.
No dogfood, full release, extracted-package certification, new native operation
crash matrix or two-node native-driver reload test was run. Those remain pending.

## Handoff

Review the four real-Git reproductions and resolve the landing primitive/profile.
Then implement the full schema/service/native driver, identity/binding hydration,
actual combined prefix/final commands, reconciliation and fencing described in
N04-recon.md. Keep the new characterization clearly separate from full acceptance
coverage. Do not mark N04 complete from this commit.
