#!/usr/bin/env bash
# The relay-call hand run, against the private controller `env.sh up rust` started, with the
# Rust test app as its runner.
#
#   wip/tools/walk-calls.sh gap    # spawn_requester: a spawn's handle says spawned or
#                                  # delivered, and the session's output reaches its sink only
#   wip/tools/walk-calls.sh run    # call_orders -> pricing; call_gateway -> its sessions,
#                                  # first as a runner without `spawner`, then with it
#
# Output goes to $OUT (default wip/tools/.cache/walk/calls); the runner's own output to
# $RUN/logs/runner.log.
set -uo pipefail

NS=calls
OUT=${WALK_OUT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)/wip/tools/.cache/walk/calls}
# shellcheck source=walk-lib.sh
source "$(dirname "$0")/walk-lib.sh"

# What a relay's sinks emit while `$2` is sent to it on its entry channel `in`.
send_and_listen() {
  local op=$1 text=$2 out=$OUT/$1-$3.txt
  flz relay listen "$op" --namespace "$NS" --duration-ms 3000 >"$out" 2>&1 &
  local listener=$!
  sleep 1
  flz relay send "$op" --namespace "$NS" --channel in --text "$text" >>"$OUT/send.txt" 2>&1
  wait "$listener"
  echo "== $op <- $text"; cat "$out"
}

gap() {
  start_relay spawn_requester spawner
  send_and_listen spawner session-1 gap
  echo "== the runner's spawn handles"; grep spawn_result "$RUN/logs/runner.log"
}

run() {
  start_relay call_pricing pricing
  start_relay call_orders orders
  send_and_listen orders o1 priced
  start_relay call_gateway gateway
  send_and_listen gateway anna-1:hello refused
  flz role bind "runner@$NS" spawner "$NS" >"$OUT/bind.txt" 2>&1
  send_and_listen gateway anna-1:hello created
  send_and_listen gateway anna-1:again delivered
  send_and_listen gateway resume:bob-1:hi absent
}

admin
[ -f "$RUN/runner.pid" ] || start_runner
case "${1:-}" in
  gap) gap ;;
  run) run ;;
  *) echo "usage: walk-calls.sh gap | run" >&2; exit 2 ;;
esac
