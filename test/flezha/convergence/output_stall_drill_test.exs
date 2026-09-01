defmodule Flezha.Convergence.OutputStallDrillTest do
  @moduledoc """
  Induced-wedge drill for `close-control-plane-silent-wedges` D1: a real
  stalled Entity, a real Watchdog sweep, a real `Convergence.Loop`, measuring
  how long the remedy decision takes now that the Watchdog no longer fires one
  itself.

  What the deletion changed is exactly one hop. Detection is unchanged — both
  the old direct resync and the new ladder start from the same sweep — so the
  question the drill answers is what that hop costs. It measures
  sweep-completed → the Loop's tick, and asserts the tick is triggered BY the
  event rather than waiting out `tick_ms` (the tick interval here is set far
  above the measurement window, so a passing measurement can only come from
  the event path).

  The numbers this run produces are recorded in the change's `design.md`.
  """

  # The Loop and the Watchdog are registered singletons; the state root is an env key.
  # async: false: writes Application env; a shared on-disk root; registered singletons.
  use ExUnit.Case, async: false

  alias Flezha.Convergence.Loop
  alias Flezha.State.{Supervisor, TestHelpers, WalWriter, Watchdog}

  @stall_ms 20
  # Far above the measurement window: a tick observed inside it is the
  # event-triggered one, never the periodic one.
  @tick_ms 3_600_000
  # The old path resynced inside the sweep (0 ms after detection). The whole
  # regression risk of the deletion is that this hop is slow; a decision hop
  # measured in single-digit milliseconds is not.
  @budget_ms 500

  setup do
    TestHelpers.tmp_app_data_dir()

    for spec <- [
          {Elixir.Registry, keys: :unique, name: Supervisor.registry_name()},
          Supervisor,
          Flezha.State.FsyncBatcher,
          Watchdog
        ] do
      case start_supervised(spec) do
        {:ok, _} -> :ok
        {:error, {:already_started, _}} -> :ok
      end
    end

    on_exit(TestHelpers.override_env(:default_output_stall_timeout_ms, @stall_ms))

    # Listening BEFORE the Loop starts: `Loop.init/1` schedules its first tick
    # immediately, and a synchronous call cannot drain a timer message that has
    # not been delivered yet. The boot tick is consumed here as the settle
    # signal, so the ticks the tests see are the ones their own sweeps cause.
    parent = self()
    handler = "stall-drill-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:flezha, :convergence, :tick],
      fn _e, meas, _meta, _ -> send(parent, {:tick, meas}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    start_supervised!({Loop, tick_ms: @tick_ms})
    assert_receive {:tick, _boot_tick}, @budget_ms

    %{header: TestHelpers.stub_header()}
  end

  test "a stalled Entity reaches the Loop's remedy decision in one hop", %{header: header} do
    {_entity_id, _writer} = stalled_writer("drill_", header)

    started_at = System.monotonic_time(:microsecond)
    :ok = Watchdog.sweep_now()

    assert_receive {:tick, tick}, @budget_ms
    elapsed_us = System.monotonic_time(:microsecond) - started_at

    assert elapsed_us < @budget_ms * 1_000,
           "output stall → remedy decision took #{Float.round(elapsed_us / 1000, 2)} ms, " <>
             "budget #{@budget_ms} ms (tick body #{tick.duration_ms} ms, " <>
             "tick interval #{@tick_ms} ms)"

    # The stall is recorded on the actor, which is what walks the ladder; the
    # sensor keeps no remediation bookkeeping of its own any more.
    state = :sys.get_state(Loop)
    assert Map.has_key?(state.stalled_operatives, "TestOperative")
  end

  test "re-flagging an already-recorded stall costs no extra tick", %{header: header} do
    {_entity_id, _writer} = stalled_writer("drill_dedup_", header)

    :ok = Watchdog.sweep_now()
    assert_receive {:tick, _}, @budget_ms

    # The sensor re-flags every sweep; the Watchdog's deleted `recently_resynced?`
    # throttle now lives on the actor as "already recorded", so a second sweep
    # must not drive the whole loop again. The sweep's re-flag reaches the Loop
    # before the call below does, and a tick it caused would have sent its
    # telemetry before the Loop answered.
    :ok = Watchdog.sweep_now()
    _ = :sys.get_state(Loop)
    refute_received {:tick, _}
  end

  # A live writer with Changes still arriving and its seq frozen — the stall
  # signature — with the advance window already lapsed on its own clock.
  defp stalled_writer(prefix, header) do
    entity_id =
      TestHelpers.sample_entity_id(
        prefix <> Integer.to_string(System.unique_integer([:positive]))
      )

    {:ok, _} = Supervisor.find_or_start(entity_id)
    {:ok, writer} = Supervisor.lookup(entity_id)
    :ok = WalWriter.append(writer, 0, header, "z")
    :ok = WalWriter.append(writer, 1, header, "a")
    TestHelpers.age_writer(writer, @stall_ms * 2)
    :duplicate = WalWriter.append(writer, 1, header, "a")
    {entity_id, writer}
  end
end
