#!/usr/bin/env bash
set -u
cd "$(dirname "$0")/../.."
OUT=bench/bounded/reports
FILTER='intent mismatch|Connection closed|compact_cell|SignalHandler|authz|Setting up|Started 2'
run() {
  local name="$1"; shift
  echo "=== $name: mix bench.bounded $* ($(date +%H:%M:%S))"
  timeout 600 mix bench.bounded "$@" 2>&1 | grep -Ev "$FILTER" > "$OUT/$name.log"
  grep -E '=== |restored|→|p50|rehydrations|parked|beam|heap|convergence|RESULT|STALLED' "$OUT/$name.log" | grep -v '^\s*$' | cut -c1-240
  sleep 3
}
run final3_sleep_1k_keep    --scenario sleep -n 1000 --keep --no-wake --stall-timeout 120
run final3_sleep_1k_boot    --scenario sleep-boot -n 1000 --no-wake --stall-timeout 120
echo "=== batch 7 done $(date +%H:%M:%S)"
