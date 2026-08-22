#!/usr/bin/env bash
# Workflow bench matrix, batch 1: chain sweep, turns, kill. One log per cell.
set -u
cd "$(dirname "$0")/../.."
OUT=bench/workflow/reports
FILTER='intent mismatch|Connection closed|compact_entity|SignalHandler|authz|Setting up|Started 2'
run() {
  local name="$1"; shift
  echo "=== $name: mix bench.workflow $* ($(date +%H:%M:%S))"
  timeout 1800 mix bench.workflow "$@" 2>&1 | grep -Ev "$FILTER" > "$OUT/$name.log"
  grep -A20 '=== .* summary' "$OUT/$name.log" | grep -v '^\s*$'
  sleep 3
}
run chain_p1_burst      --scenario chain -n 1000 --procs 1 --stall-timeout 120
run chain_p4_burst      --scenario chain -n 1000 --procs 4 --stall-timeout 120
run chain_p16_burst     --scenario chain -n 1000 --procs 16 --stall-timeout 120
run chain_optout_burst  --scenario chain -n 1000 --procs 4 --tier optout --stall-timeout 120
run chain_p4_c10        --scenario chain -n 1000 --procs 4 -c 10 --stall-timeout 120
run chain_p4_c100       --scenario chain -n 2000 --procs 4 -c 100 --stall-timeout 120
run chain_p4_c1000      --scenario chain -n 5000 --procs 4 -c 1000 --stall-timeout 180 --status-every 15
run chain_p4_rate50     --scenario chain -n 1000 --procs 4 --rate 50 --stall-timeout 120
run chain_p4_rate200    --scenario chain -n 2000 --procs 4 --rate 200 --stall-timeout 120
run turns_1x500         --scenario turns -n 1 -m 500 --stall-timeout 120
run turns_100x100       --scenario turns -n 100 -m 100 --stall-timeout 120
run turns_1000x20       --scenario turns -n 1000 -m 20 --stall-timeout 180 --status-every 15
run turns_100_rate200   --scenario turns -n 100 -m 50 --rate 200 --stall-timeout 120
run turns_churn         --scenario turns -n 200 -m 20 --rate 100 --park-idle-ms 500 --stall-timeout 120
run turns_kill          --scenario turns -n 100 -m 60 --rate 200 --kill-runner-at 10 --stall-timeout 180
echo "=== batch 1 done $(date +%H:%M:%S)"
