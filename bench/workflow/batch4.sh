#!/usr/bin/env bash
# Proof cells after the late fixes: the completion teardown drain (3/1000 outputs
# lost before) and the process_hub reconcile two-rounds rule (local checkout via
# FLEZHA_PROCESS_HUB_PATH), on the cells that showed them.
set -u
cd "$(dirname "$0")/../.."
OUT=bench/workflow/reports
FILTER='intent mismatch|Connection closed|SignalHandler|authz|Setting up|Started 2'
export FLEZHA_PROCESS_HUB_PATH=/home/moon/Code/process-hub
run() {
  local name="$1"; shift
  echo "=== $name: mix bench.workflow $* ($(date +%H:%M:%S))"
  timeout 1800 mix bench.workflow "$@" 2>&1 | grep -Ev "$FILTER" > "$OUT/$name.log"
  grep -A22 '=== .* summary' "$OUT/$name.log" | grep -v '^\s*$'
  sleep 3
}
run fixed2_chain_p4_burst   --scenario chain -n 1000 --procs 4 --stall-timeout 120
run fixed2_chain_p4_c1000   --scenario chain -n 5000 --procs 4 -c 1000 --stall-timeout 180 --status-every 15
unset FLEZHA_PROCESS_HUB_PATH
mix deps.get >/dev/null 2>&1 && mix compile >/dev/null 2>&1 && echo "=== deps restored to the pinned process_hub"
echo "=== batch 4 done $(date +%H:%M:%S)"
