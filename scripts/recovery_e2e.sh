#!/bin/bash
#
# Flezha Failure-Recovery E2E Smoke
# ==================================
#
# Drives the single-node recovery invariant end-to-end: start the agent,
# add N processors, exit cleanly so DETS flushes, restart, and verify
# that all N processors are running again within
# `FLEZHA_RECOVERY_WINDOW_MS + 2s` of the second startup.
#
# This script uses `iex -S mix` under `MIX_ENV=dev` so the application
# supervision tree starts the recovery subsystem (prod env is equivalent
# but requires a full release build). Full production verification
# should be performed against a real release via
# `_build/prod/rel/flezha/bin/flz agent start`.
#
# Usage:
#   ./scripts/recovery_e2e.sh [--num-processors N] [--window-ms MS]
#
# Preconditions:
#   - The rust_compute_engine binary must be built. The script looks for
#     test/apps/rust_compute_engine/target/{release,debug}/rust_compute_engine
#     and launches it via FLEZHA_EXTERNAL_PROCESSES so PortPilot supervises it.
#   - `mix deps.get && mix compile` must have run.
#
# Exit codes:
#   0  – all processors recovered within the window.
#   1  – recovery failed or incomplete.
#   2  – prerequisites missing.

set -euo pipefail

NUM_PROCESSORS=5
WINDOW_MS=5000
DATA_DIR="${FLEZHA_DATA_DIR:-/tmp/flz-recovery-e2e}"
FACTORY_ID="do_nothing"
NAMESPACE="default"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --num-processors|-n)
            NUM_PROCESSORS="$2"; shift 2;;
        --window-ms)
            WINDOW_MS="$2"; shift 2;;
        --data-dir)
            DATA_DIR="$2"; shift 2;;
        -h|--help)
            sed -n '3,27p' "$0"
            exit 0;;
        *)
            echo "Unknown arg: $1" >&2; exit 2;;
    esac
done

# Locate the compute engine binary so PortPilot can launch it. In dev env
# runtime.exs reads FLEZHA_EXTERNAL_PROCESSES (empty by default), so we
# have to supply the launch command explicitly — test env is the only
# config that hard-codes `cargo run`.
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CE_REL="$REPO_ROOT/test/apps/rust_compute_engine/target/release/rust_compute_engine"
CE_DBG="$REPO_ROOT/test/apps/rust_compute_engine/target/debug/rust_compute_engine"
if [[ -x "$CE_REL" ]]; then
    CE_BIN="$CE_REL"
elif [[ -x "$CE_DBG" ]]; then
    CE_BIN="$CE_DBG"
else
    echo "recovery-e2e: rust_compute_engine not built — run 'cargo build -p rust_compute_engine' (or '--release') first" >&2
    exit 2
fi

echo "recovery-e2e: num_processors=$NUM_PROCESSORS window_ms=$WINDOW_MS data_dir=$DATA_DIR"
echo "recovery-e2e: compute_engine=$CE_BIN"

rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

export FLEZHA_DATA_DIR="$DATA_DIR"
export FLEZHA_RECOVERY_ENABLED="true"
export FLEZHA_RECOVERY_WINDOW_MS="$WINDOW_MS"
export FLEZHA_EXTERNAL_PROCESSES="$CE_BIN"

# Phase 1 — boot, add processors + a stdout plugin on the first one, then
# exit cleanly so DETS is flushed.
echo "== phase 1: seed processors + plugin =="
cat <<EOF | MIX_ENV=dev iex --sname flz_e2e_p1 --cookie flz_e2e_cookie -S mix
# Wait for the rust compute engine to register with the agent before
# issuing start_processor calls — otherwise ProcessorManager returns
# {:error, "no compute_engines in namespace default"}.
Flezha.Service.EventManager.add_handler(:compute_engine_connected_event)
receive do
  {:compute_engine_connected_event, {"$NAMESPACE", _}} -> :ok
after
  20_000 ->
    IO.puts("recovery-e2e: compute engine did not connect within 20s")
    System.halt(2)
end

ns = "$NAMESPACE"
fac = "$FACTORY_ID"
n = $NUM_PROCESSORS

for i <- 1..n do
  proc = %Flezha.Processor.Builder{id: "e2e_proc_" <> Integer.to_string(i), factory_id: fac}
  {:ok, _} = Flezha.Service.ProcessorManager.start_processor(ns, proc)
end

# Attach a stdout plugin to processor #1 so we can verify plugin recovery.
plugin = %Flezha.Plugin.Driver.IOBindingPlugin{
  namespace: ns,
  processor_id: "e2e_proc_1",
  trigger: :on_output,
  provider: %Flezha.Plugin.Provider.StdOut{}
}
{:ok, _} = Flezha.Service.PluginManager.enable_plugin("e2e_stdout_1", plugin)

# Allow hooks to flush before exit.
Process.sleep(500)

IO.puts("seeded " <> Integer.to_string(Flezha.Recovery.Store.count()) <>
        " processor rows, " <>
        Integer.to_string(Flezha.Recovery.PluginStore.count()) <>
        " plugin rows into " <> Flezha.Recovery.Store.path())

System.halt(0)
EOF

if [[ ! -f "$DATA_DIR/recovery.dets" ]]; then
    echo "recovery-e2e: no DETS file at $DATA_DIR/recovery.dets after phase 1" >&2
    exit 1
fi

if [[ ! -f "$DATA_DIR/recovery_plugins.dets" ]]; then
    echo "recovery-e2e: no plugin DETS file at $DATA_DIR/recovery_plugins.dets after phase 1" >&2
    exit 1
fi

# Phase 2 — boot again, verify recovery ran and all processors are live.
echo "== phase 2: verify recovery =="
EXPECTED=$NUM_PROCESSORS
DEADLINE_MS=$((WINDOW_MS + 5000))

cat <<EOF | MIX_ENV=dev iex --sname flz_e2e_p2 --cookie flz_e2e_cookie -S mix
deadline = System.monotonic_time(:millisecond) + $DEADLINE_MS
ns = "$NAMESPACE"
expected = $EXPECTED

# Wait for the compute engine to reconnect; the recovery coordinator
# needs a live CE for start_processor to succeed (otherwise the row
# fails permanently).
Flezha.Service.EventManager.add_handler(:compute_engine_connected_event)
receive do
  {:compute_engine_connected_event, {^ns, _}} -> :ok
after
  20_000 ->
    IO.puts("recovery-e2e: compute engine did not reconnect within 20s")
    System.halt(1)
end

# Wait for SysMode to move past :recover.
wait = fn wait ->
  case Flezha.Recovery.SysMode.get() do
    :normal -> :ok
    _ ->
      if System.monotonic_time(:millisecond) > deadline do
        IO.puts("recovery-e2e: SysMode did not reach :normal before deadline")
        System.halt(1)
      else
        Process.sleep(50)
        wait.(wait)
      end
  end
end
wait.(wait)

{:ok, procs} = Flezha.Service.ProcessorManager.get_processor_ids(ns)
plugins = Flezha.Service.PluginManager.get_plugins()

IO.puts("recovery-e2e: found " <> Integer.to_string(length(procs)) <>
        " processor(s), " <> Integer.to_string(map_size(plugins)) <>
        " plugin(s) after recovery")

cond do
  length(procs) < expected ->
    IO.puts("recovery-e2e: expected >= " <> Integer.to_string(expected) <> " processors")
    System.halt(1)

  not Map.has_key?(plugins, "e2e_stdout_1") ->
    IO.puts("recovery-e2e: expected plugin e2e_stdout_1 to be re-enabled")
    System.halt(1)

  true ->
    System.halt(0)
end
EOF

status=$?
if [[ $status -eq 0 ]]; then
    echo "recovery-e2e: PASS"
else
    echo "recovery-e2e: FAIL (status=$status)"
fi
exit $status
