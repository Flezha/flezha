#!/usr/bin/env bash
#
# Repeatable steps-bench runner. See bench/acceptance/README.md.
#
#   bench/acceptance/run.sh [--profile quick|standard|soak]
#                           [--repeat N] [--runners N]
#                           [--transport uds|quic] [--runtime rust|java]
#                           [--save-baseline]
#
# Runs every case in cases.<profile>.tsv `--repeat` times, prints each run's
# result as it lands, then writes a median summary and compares it against
# baseline.<profile>.tsv (regression = median throughput down >15%, or any
# non-clean integrity). --save-baseline records the current medians instead.
set -u
cd "$(dirname "$0")/../.."
A=bench/acceptance

PROFILE=standard
REPEAT=""
RUNNERS=2
TRANSPORT=uds
RUNTIME=rust
SAVE=0
while [ $# -gt 0 ]; do
  case "$1" in
    --profile)   PROFILE="$2"; shift 2 ;;
    --repeat)    REPEAT="$2"; shift 2 ;;
    --runners)   RUNNERS="$2"; shift 2 ;;
    --transport) TRANSPORT="$2"; shift 2 ;;
    --runtime)   RUNTIME="$2"; shift 2 ;;
    --save-baseline) SAVE=1; shift ;;
    -h|--help)   sed -n '2,16p' "$0"; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

CASES="$A/cases.$PROFILE.tsv"
[ -f "$CASES" ] || { echo "no such profile: $CASES" >&2; exit 2; }
# Default repeats per profile: soak runs once (long), others 3.
[ -n "$REPEAT" ] || REPEAT=$([ "$PROFILE" = soak ] && echo 1 || echo 3)

RESULTS="$A/results.$PROFILE.tsv"
: > "$RESULTS"
FILTER='intent mismatch|Connection closed|compact_cell|SignalHandler|authz|Setting up|Started|plugin-degraded|task_slow'

echo "== acceptance: profile=$PROFILE repeat=$REPEAT runners=$RUNNERS transport=$TRANSPORT runtime=$RUNTIME  $(date '+%F %T') =="
echo "   (one controller — multi-controller/multi-node is a separate harness; see README)"

grep -vE '^\s*#|^\s*$' "$CASES" | while IFS=$'\t' read -r name case_rt args; do
  rt="$RUNTIME"; [ "$case_rt" != default ] && rt="$case_rt"
  for r in $(seq 1 "$REPEAT"); do
    printf '[%-14s %d/%d rt=%s] ' "$name" "$r" "$REPEAT" "$rt"
    line=$(FLEZHA_TRANSPORT="$TRANSPORT" timeout 5400 \
             mix bench.steps --runtime "$rt" --runners "$RUNNERS" $args 2>&1 \
             | grep -Ev "$FILTER" | grep '^BENCH_RESULT' | tail -1)
    if [ -z "$line" ]; then
      echo "NO RESULT (stall/crash)"
      printf '%s\t%s\tthroughput=0\tintegrity=stall\n' "$name" "$rt" >> "$RESULTS"
    else
      echo "${line#BENCH_RESULT }" | awk '{
        for (i=1; i<=NF; i++) { split($i, a, "="); v[a[1]] = a[2] }
        printf "%s %s · p50 %s ms · peak %s MiB · fsync %s ms · %s\n",
               v["throughput"], v["unit"], v["p50_ms"], v["peak_mb"], v["fsync_p99_ms"], v["integrity"]
      }'
      printf '%s\t%s\t%s\n' "$name" "$rt" "${line#BENCH_RESULT }" >> "$RESULTS"
    fi
  done
done

echo
echo "== summary (profile=$PROFILE) =="
elixir "$A/summarize.exs" "$RESULTS" "$A/cases.$PROFILE.tsv" "$A/baseline.$PROFILE.tsv" "$SAVE"
