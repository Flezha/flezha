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

ROOT=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
ENV=$ROOT/wip/tools/env.sh
RUN=$ROOT/wip/tools/.cache/run
OUT=${WALK_OUT:-$ROOT/wip/tools/.cache/walk/parts}
NS=parts
OP=pe
[ -f "$RUN/vars.sh" ] || { echo "walk: run wip/tools/env.sh up rust first" >&2; exit 1; }
# shellcheck disable=SC1091
source "$RUN/vars.sh"
mkdir -p "$OUT"

# The admin is claimed once per controller; its token is kept beside the run.
admin() {
  if [ ! -f "$RUN/admin.token" ]; then
    (cd "$RUN/ctl" && flz auth bootstrap "$(cat app_data/auth/bootstrap.token)") >"$OUT/bootstrap.txt" 2>&1 ||
      { cat "$OUT/bootstrap.txt" >&2; exit 1; }
    sed -n 's/.*(shown once, store it now): //p' "$OUT/bootstrap.txt" >"$RUN/admin.token"
  fi
  FLEZHA_TOKEN=$(cat "$RUN/admin.token")
  export FLEZHA_TOKEN
}

# The runner proves its namespace with a runner key, issued once per run.
runner_key() {
  flz namespace create "$NS" >/dev/null 2>&1
  [ -f "$RUN/runner.key" ] ||
    flz token issue "runner@$NS" "$NS" runner --ttl never | grep -o 'flzt_[a-z0-9]*' | head -1 >"$RUN/runner.key"
  cat "$RUN/runner.key"
}

runner() {
  [ -f "$RUN/runner.pid" ] && return 0
  cargo build --quiet --manifest-path="$ROOT/sdk/rust/test_app/Cargo.toml" --target-dir="$ROOT/target" ||
    exit 1
  "$ENV" start runner "$ROOT" env FLEZHA_NAMESPACE_SECRET="$(runner_key)" \
    "$ROOT/target/debug/rust_runner" "$NS"
  for _ in $(seq 120); do
    flz operator start part_echo --id "$OP" --namespace "$NS" >"$OUT/start.txt" 2>&1 && return 0
    sleep 0.5
  done
  cat "$OUT/start.txt" >&2
  exit 1
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
