#!/usr/bin/env bash
# Final re-take of the wake/rebind-storm cases on the complete fix set (the
# batch-3 runs of these predate the un-park race and restore-seq fixes).
set -u
cd "$(dirname "$0")/../.."
OUT=bench/bounded/reports
FILTER='intent mismatch|Connection closed|compact_cell|SignalHandler|authz|Setting up|Started 2'
run() {
  local name="$1"; shift
  echo "=== $name: mix bench.bounded $* ($(date +%H:%M:%S))"
  timeout 1800 mix bench.bounded "$@" 2>&1 | grep -Ev "$FILTER" > "$OUT/$name.log"
  grep -A22 '=== .* summary' "$OUT/$name.log" | grep -v '^\s*$'
  sleep 3
}
run final_turns_churn       --scenario turns -n 200 -m 20 --rate 100 --park-idle-ms 500 --stall-timeout 120
run final_turns_100_rate200 --scenario turns -n 100 -m 50 --rate 200 --stall-timeout 120
run final_turns_kill        --scenario turns -n 100 -m 60 --rate 200 --kill-runner-at 10 --stall-timeout 180
run final_turns_1000x20     --scenario turns -n 1000 -m 20 --stall-timeout 180 --status-every 15
run final_sleep_1k_storm    --scenario sleep -n 1000 --stall-timeout 120
run final_sleep_1k_keep     --scenario sleep -n 1000 --wake-rate 20 --keep --stall-timeout 120
run final_sleep_1k_boot     --scenario sleep-boot -n 1000 --wake-rate 20 --stall-timeout 120
echo "=== batch 5 done $(date +%H:%M:%S)"
