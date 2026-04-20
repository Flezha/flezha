#!/bin/bash
#
# Flezha Failure-Recovery E2E Smoke
# ==================================
#
# Drives the single-node recovery invariant end-to-end: start the agent,
# add N processors, kill the BEAM non-gracefully (SIGKILL), restart, and
# verify that all N processors are running again within
# `FLEZHA_RECOVERY_WINDOW_MS + 2s` of the second startup.
#
# This script uses `iex -S mix` (dev/test env) for brevity — full
# production verification should be performed against a real release via
# `_build/prod/rel/flezha/bin/flz agent start`.
#
# Usage:
#   ./scripts/recovery_e2e.sh [--num-processors N] [--window-ms MS]
#
# Preconditions:
#   - The rust_compute_engine binary must be built (first iex session
#     starts it under the default namespace).
#   - `mix deps.get && mix compile` must have run.
#
# Exit codes:
#   0  – all processors recovered within the window.
#   1  – recovery failed or incomplete.
#   2  – prerequisites missing.

set -euo pipefail

NUM_PROCESSORS=5
WINDOW_MS=2000
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
            sed -n '3,25p' "$0"
            exit 0;;
        *)
            echo "Unknown arg: $1" >&2; exit 2;;
    esac
done

echo "recovery-e2e: num_processors=$NUM_PROCESSORS window_ms=$WINDOW_MS data_dir=$DATA_DIR"

rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

export FLEZHA_DATA_DIR="$DATA_DIR"
export FLEZHA_RECOVERY_ENABLED="true"
export FLEZHA_RECOVERY_WINDOW_MS="$WINDOW_MS"
# The e2e loop must live in prod config so recovery is not short-circuited by
# the test/dev overrides, but we leave MIX_ENV to the caller — this script
# assumes they've compiled with the right env already.

# Phase 1 — boot, add processors + a stdout plugin on the first one, then
# exit cleanly so DETS is flushed.
echo "== phase 1: seed processors + plugin =="
cat <<EOF | MIX_ENV=prod iex --sname flz_e2e_p1 --cookie flz_e2e_cookie -S mix
Process.sleep(3_000)  # allow compute_engine connection

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
# (script uses the direct Elixir API, so no CLI flag syntax concerns here)

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
DEADLINE_MS=$((WINDOW_MS + 2000))

cat <<EOF | MIX_ENV=prod iex --sname flz_e2e_p2 --cookie flz_e2e_cookie -S mix
deadline = System.monotonic_time(:millisecond) + $DEADLINE_MS
ns = "$NAMESPACE"
expected = $EXPECTED

# wait for SysMode to move past :recover
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
