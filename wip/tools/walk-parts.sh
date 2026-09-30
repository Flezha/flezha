#!/usr/bin/env bash
# The parts hand run (`send-a-part-while-it-runs` 6.4) and the listener
# attach/detach timing (7.4), against the private controller `env.sh up rust`
# started, with the Rust test app as its runner.
#
#   wip/tools/walk-parts.sh run      # part_echo: listen in one shell, send 5 in another
#   wip/tools/walk-parts.sh timing   # 200 sequential attach+detach on one operator
#
# Output goes to $OUT (default wip/tools/.cache/walk/parts): listen.txt and
# send.txt for `run`, timing.txt for `timing`; the runner's own output to
# $RUN/logs/runner.log.
set -uo pipefail

NS=parts
OP=pe
OUT=${WALK_OUT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)/wip/tools/.cache/walk/parts}
# shellcheck source=walk-lib.sh
source "$(dirname "$0")/walk-lib.sh"

runner() {
  [ -f "$RUN/runner.pid" ] && return 0
  start_runner
  start_operator part_echo "$OP"
}

run() {
  flz operator listen "$OP" --namespace "$NS" --duration-ms 10000 >"$OUT/listen.txt" 2>&1 &
  local listener=$!
  sleep 2
  flz operator send "$OP" --namespace "$NS" --text 5 >"$OUT/send.txt" 2>&1
  wait "$listener"
  echo "== listen"; cat "$OUT/listen.txt"
  echo "== send"; cat "$OUT/send.txt"
}

timing() {
  flzvm rpc '
    ns = "'"$NS"'"
    samples =
      for _ <- 1..200 do
        started = System.monotonic_time(:microsecond)
        {:ok, :window_closed} = Flezha.Service.Listener.listen(ns, "'"$OP"'", [window_ms: 1], fn _ -> :continue end)
        System.monotonic_time(:microsecond) - started
      end
    sorted = Enum.sort(samples)
    IO.puts("n=#{length(sorted)} mean_ms=#{Float.round(Enum.sum(sorted) / length(sorted) / 1000, 2)} " <>
      "p50_ms=#{Float.round(Enum.at(sorted, 99) / 1000, 2)} p99_ms=#{Float.round(Enum.at(sorted, 197) / 1000, 2)} " <>
      "max_ms=#{Float.round(List.last(sorted) / 1000, 2)}")
  ' | tee "$OUT/timing.txt"
}

admin
runner
case "${1:-}" in
  run) run ;;
  timing) timing ;;
  *) echo "usage: walk-parts.sh run | timing" >&2; exit 2 ;;
esac
