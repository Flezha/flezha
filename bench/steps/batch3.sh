#!/usr/bin/env bash
# A/B re-run of the batch-1 cases that collapsed, on the fixed code.
set -u
cd "$(dirname "$0")/../.."
OUT=bench/steps/reports
FILTER='intent mismatch|Connection closed|compact_cell|SignalHandler|authz|Setting up|Started 2'
run() {
  local name="$1"; shift
  echo "=== $name: mix bench.steps $* ($(date +%H:%M:%S))"
  timeout 1800 mix bench.steps "$@" 2>&1 | grep -Ev "$FILTER" > "$OUT/$name.log"
  grep -A22 '=== .* summary' "$OUT/$name.log" | grep -v '^\s*$'
  sleep 3
}
run fixed_chain_p4_burst    --scenario chain -n 1000 --tasks 4 --stall-timeout 120
run fixed_chain_p4_c100     --scenario chain -n 2000 --tasks 4 -c 100 --stall-timeout 120
run fixed_chain_p4_rate50   --scenario chain -n 1000 --tasks 4 --rate 50 --stall-timeout 120
run fixed_turns_100x100     --scenario turns -n 100 -m 100 --stall-timeout 120
run fixed_turns_100_rate200 --scenario turns -n 100 -m 50 --rate 200 --stall-timeout 120
run fixed_turns_churn       --scenario turns -n 200 -m 20 --rate 100 --park-idle-ms 500 --stall-timeout 120
run fixed_turns_kill        --scenario turns -n 100 -m 60 --rate 200 --kill-runner-at 10 --stall-timeout 180
echo "=== batch 3 done $(date +%H:%M:%S)"
