#!/usr/bin/env bash
# Bounded operator bench matrix, batch 2: the sleep family (rest, wake storms, boot), a churn soak, repro cases.
set -u
cd "$(dirname "$0")/../.."
OUT=bench/bounded/reports
FILTER='intent mismatch|Connection closed|compact_cell|SignalHandler|authz|Setting up|Started 2'
run() {
  local name="$1"; shift
  echo "=== $name: mix bench.bounded $* ($(date +%H:%M:%S))"
  timeout 3600 mix bench.bounded "$@" 2>&1 | grep -Ev "$FILTER" > "$OUT/$name.log"
  grep -E '=== |→|p50|rehydrations|parked|restored|beam|processes|state dir|convergence|timer wakes|commits|RESULT|STALLED|integrity|throughput|casts out|journal=' "$OUT/$name.log" | grep -v '^\s*$' | cut -c1-240
  sleep 3
}
run sleep_3k_keep       --scenario sleep -n 3000 --spawn-concurrency 50 --wake-rate 50 --keep --stall-timeout 180 --status-every 30
run sleep_3k_boot       --scenario sleep-boot -n 3000 --wake-rate 50 --stall-timeout 180 --status-every 30
run soak_churn_15m      --scenario turns -n 100 --duration 900 --rate 20 --park-idle-ms 500 --stall-timeout 180 --status-every 60
# Single-loop wedge repro: with the park impossible (60 s idle) vs the default 5 s park.
run turns_1x500_nopark  --scenario turns -n 1 -m 500 --park-idle-ms 60000 --stall-timeout 60
run turns_1x500_repro   --scenario turns -n 1 -m 500 --stall-timeout 60
echo "=== batch 2 done $(date +%H:%M:%S)"
