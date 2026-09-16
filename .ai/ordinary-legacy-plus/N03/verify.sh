#!/usr/bin/env bash
set -u
root=.ai/ordinary-legacy-plus/N03
failed=0
run_check() {
  local label=$1
  shift
  printf '%q ' "$@" > "$root/$label.command"
  printf '\n' >> "$root/$label.command"
  /usr/bin/time -f 'exit=%x duration_seconds=%e' -o "$root/$label.time" "$@" > "$root/$label.log" 2>&1
  local code=$?
  printf '%s exit=%s\n' "$label" "$code"
  if (( code != 0 )); then failed=1; tail -60 "$root/$label.log"; fi
}
case "${1:-focused}" in
  focused)
    run_check lifecycle-final node scripts/dag-v2-lifecycle-test.mjs
    run_check state-final node scripts/dag-v2-state-test.mjs
    run_check diff-final git diff --check
    ;;
  regression)
    run_check model node scripts/project-model-test.mjs
    run_check planning node scripts/dag-planning-test.mjs
    run_check runtime node scripts/dag-runtime-test.mjs
    run_check workers node scripts/worker-runtime-test.mjs
    run_check git node scripts/git-integration-test.mjs
    run_check planning-runtime node scripts/dag-planning-runtime-test.mjs
    ;;
esac
exit "$failed"
