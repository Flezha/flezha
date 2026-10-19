#!/usr/bin/env bash
# A section's net production lines: prod-lines.sh at the section's start and at
# HEAD, minus every commit in between that is not the section's (a peer's, or
# another row's), each measured against its parent. Runs in a scratch worktree
# so the working tree is never touched.
#
#   wip/tools/section-prod-lines.sh <start-commit> [other-commit ...]
set -euo pipefail
ROOT=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
START=$1; shift
WT=$(mktemp -d "${TMPDIR:-/tmp}/section-lines.XXXXXX")
trap 'git -C "$ROOT" worktree remove --force "$WT" >/dev/null 2>&1 || true' EXIT
git -C "$ROOT" worktree add -q --detach "$WT" "$START"
measure() {
  git -C "$WT" checkout -q --detach "$1"
  git -C "$WT" -c protocol.file.allow=always submodule update -q --init
  bash "$WT/wip/tools/prod-lines.sh" | awk '/^total/ {print $2}'
}
start=$(measure "$START")
head=$(measure "$(git -C "$ROOT" rev-parse HEAD)")
others=0
for c in "$@"; do
  delta=$(( $(measure "$c") - $(measure "$c^") ))
  echo "not the section's: $c $delta"
  others=$((others + delta))
done
echo "start $start  head $head  others $others"
echo "section net $((head - start - others))"
