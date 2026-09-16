#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../../../.."
logs="${1:-$(mktemp -d /tmp/n03-subreaper-rerun-XXXXXX)}"
mkdir -p "$logs"
logs="$(realpath "$logs")"
printf 'Evidence directory: %s\n' "$logs"
failed=0
for label in context lifecycle state; do
  printf 'timeout 3600 node scripts/dag-v2-%s-test.mjs\n' "$label" > "$logs/$label.command"
  if /usr/bin/time -f 'exit=%x duration_seconds=%e' -o "$logs/$label.time" \
    timeout 3600 node "scripts/dag-v2-$label-test.mjs" > "$logs/$label.log" 2>&1; then
    printf '%s: PASS\n' "$label"
  else
    failed=1
    printf '%s: FAIL (see %s)\n' "$label" "$logs/$label.log"
  fi
done
exit "$failed"
