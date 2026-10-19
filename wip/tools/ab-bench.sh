#!/usr/bin/env bash
# An interleaved A/B of `mix bench`: A, B, A, B, ... in two trees, each run
# under the private settings `env.sh vars` writes, raw output kept per run.
#
#   wip/tools/ab-bench.sh <tree A> <tree B> <rounds> <out dir> [mix bench args...]
#
# Tree A is usually a worktree of the base commit. Each tree builds what it
# runs on its first bench. Prints every run's end-to-end and receive-only
# msg/s and the median of each arm; raw output lands in <out dir>/<arm>-<n>.log.
set -Eeuo pipefail

[ $# -ge 4 ] || { sed -n '2,10p' "$0"; exit 2; }
A=$(cd "$1" && pwd) B=$(cd "$2" && pwd) ROUNDS=$3 OUT=$4
shift 4
ROOT=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
mkdir -p "$OUT"
# shellcheck disable=SC1090
source "$("$ROOT/wip/tools/env.sh" vars "$OUT/vars")"

rate() { # log, window
  awk -v w="$2" '$0 ~ w {print $NF}' "$1" | tr -d ',' | tail -1
}

median() { sort -n | awk '{a[NR]=$1} END {print (NR % 2) ? a[(NR+1)/2] : (a[NR/2] + a[NR/2+1]) / 2}'; }

for n in $(seq "$ROUNDS"); do
  for arm in A B; do
    tree=$A
    [ "$arm" = B ] && tree=$B
    log=$OUT/$arm-$n.log
    (cd "$tree" && timeout 1200 mix bench "$@") >"$log" 2>&1 || echo "$arm-$n failed, see $log" >&2
    echo "$arm $n end-to-end $(rate "$log" 'end-to-end') receive-only $(rate "$log" 'receive only')"
  done
done | tee "$OUT/runs.txt"

for arm in A B; do
  for window in end-to-end receive-only; do
    m=$(awk -v a="$arm" -v w="$window" '$1 == a {for (i = 3; i < NF; i++) if ($i == w) print $(i + 1)}' \
      "$OUT/runs.txt" | median)
    echo "median $arm $window $m"
  done
done | tee "$OUT/medians.txt"
