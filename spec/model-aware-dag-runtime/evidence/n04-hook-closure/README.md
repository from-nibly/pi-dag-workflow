# N04 configured-hook isolation and closure binding evidence

Base: `2987c750a5d88173e74549b0afbcea9f370ad63e` (joined attribute, hidden-dirty,
and awaited launch-cleanliness fixes). Branch: `implement/legacy-plus-git-hooks`.
This is bounded fix evidence, not extracted-package or full release certification.

## Changes

- Git 2.54 configured hooks bypass `core.hooksPath`. Native operations now
  discover hook names through Git's config parser on each invocation and disable
  each name at command scope. Discovery includes local/worktree config, nested
  relative includes, and dormant conditional includes that may activate in
  worktree-add children. The name override supports case, spaces, dots and `=`.
  Config files are not rewritten; explicitly disabled and unused pre-push hooks
  are allowed, rather than banning unrelated config.
- Native-profile validation and landing descendants inherit per-name disabling.
  Later native observation/cleanup/recovery/closure rediscovers names installed
  by validation; the runner does not cache hook-disabling options. Landing still
  runs the operation-owned expected-old/new/direct-target guard under Git's ref
  locks. There is no replacement with prechecked plain fast-forward.
- Fresh `closeOperation` verifies persisted native identity before creating or
  opening the common lock, then verifies native binding and held-lock identity
  again before claiming. The regression snapshots an unrelated replacement's
  entire common-directory contents and the operation store: rejection changes
  neither and creates no lock, claim, temporary file or other metadata.

Actual marker regressions cover dirty initial rejection, old/new/third native
observations, private refs, post-checkout/post-merge, complete real driver runs,
fresh old/new recovery, composed closure, config installed at durable landing
intent, validation-installed local/include/worktree config, validation descendants,
and a runner constructed before hook installation. Raw control invocations prove
that configured hooks fire without the isolation. Existing worktree-config profile
rejection is retained: validation-installed worktree config is BLOCKED and the
workspace retained, with no hook marker.

## Verification

All final invocations used **3600-second process budgets** and exited **0**.
Commands, monotonic durations, source SHA-256s, log SHA-256s and original log
locations are in [`results.json`](results.json). Logs are checked in alongside it.

| Invocation (`node scripts/…`) | Count | Seconds | Log |
| --- | ---: | ---: | --- |
| `dag-v2-git-hooks-test.mjs` | 18/18 | 15.804 | [hooks](hooks.log) |
| `dag-v2-git-acceptance-test.mjs --test-name 'configured hooks:'` | 9/9 | 233.910 | [focused hooks](focused-driver-hooks.log) |
| `dag-v2-git-acceptance-test.mjs --test-name 'fresh closure rejects replacement'` | 1/1 | 18.072 | [focused closure](focused-closure.log) |
| `dag-v2-git-acceptance-test.mjs` | 61/61 | 913.569 | [native](native.log) |
| `dag-v2-lifecycle-test.mjs` | 38/38 | 150.660 | [lifecycle](lifecycle.log) |
| `dag-v2-workspace-test.mjs` | 19/19 | 110.432 | [workspace](workspace.log) |
| `dag-v2-git-attributes-test.mjs` | 24/24 | 30.618 | [attributes](attributes.log) |
| `dag-v2-git-test.mjs` | 4 characterizations | 0.758 | [plumbing](git-plumbing.log) |

The focused driver/closure cases are subsets of the full native suite, not extra
unique cases. The original plumbing suite characterizes unsafe unguarded merge;
its historical “BLOCKED, not certified” footer is not a failure or certification
of this implementation. `git diff --check` also passed. No full typecheck,
installed-overlay, package extraction, or release-readiness sweep was performed.

Two preliminary test assertions failed and were corrected: Git rejects newline
config keys in fixture setup (replaced with a legal case/dot/equals/space name),
and the expected multi-line worktree-config rejection required a dot-all regexp.
Their original failed logs/metadata are retained as `*-initial.*`; neither is
counted as a pass. All final suites above passed after the corrections.

## Old review runner and constraints

PID 267847 had exited and `/tmp/n04-review-a5649e0600/done` existed with all eight
old-tree suites recorded as exit 0. Those results are diagnostic **only**, not fix
certification; see [`old-runner-diagnostic.json`](old-runner-diagnostic.json).
Only its known owned `node_modules` symlink was removed after confirming the
runner was gone and no process cwd was in that old workspace. No tracked file in
the original review workspace was edited.

Verification used an owned dependency symlink to already-present dependencies;
there were no installs, installed-package changes, external publication/network
operations, DAG orchestration, or project-model authority mutations. Test effects
were limited to owned disposable repositories/workspaces and evidence files.

## Remaining limits

This is the existing Linux/Git 2.54 files-ref, quiescent-checkout native profile.
Hook inventory is per invocation and costs additional metadata-only Git calls;
referenced config must be readable/parseable, including dormant includes.
Concurrent same-UID config changes between inventory and exec are not isolated.
Native-profile validation argv remains trusted local code: it can deliberately
override inherited settings or install and itself invoke a new hook inside argv.
The fix prevents inherited pre-existing hooks and later unapproved native
observation/cleanup execution; it is not an argv sandbox. Existing unsupported
worktree-config/attribute restrictions and non-native runner behavior are not
expanded into a general Git security profile.
