defmodule FlezhaTest.Runner.ControlChannelLivenessTest do
  @moduledoc """
  Control-plane liveness watchdog (unify-executor-liveness-and-identity, D1).

  `Flezha.Runner.ControlChannel` stamps a monotonic `last_seen` on every
  inbound control frame — including the SDK keepalive ping it formerly echoed
  and discarded — and a jittered watchdog treats sustained silence past the
  window as a connection close, routing through the SAME
  `handle_connection_closed/1` a real `{:flz_closed}` uses. This closes the UDS
  wedged-but-connected blind spot.

  These are deterministic handler-level tests (the watchdog decision is driven
  directly, like `ControlUnitHealthTest` drives `:heartbeat`): they never shrink
  the global window via `put_env`, which would falsely evict the suite's
  long-lived default-namespace runners.
  """
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Flezha.Runner.ControlChannel

  setup do
    test_pid = self()
    handler_id = "liveness-test-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:flezha, :runner, :liveness_timeout],
      fn _event, measurements, metadata, _ ->
        send(test_pid, {:liveness_timeout, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok
  end

  defp window_ms, do: Application.get_env(:flezha, :control_liveness_window_ms, 45_000)

  defp state(runner_id, last_seen) do
    %{
      namespace: "liveness-watchdog-test-ns",
      runner_id: runner_id,
      stream_ref: :fake_ref,
      write_fun: fn _ref, _payload -> :ok end,
      control_unit: nil,
      last_seen: last_seen
    }
  end

  test "an idle wedged-but-connected runner is declared dead via the watchdog" do
    # No inbound frame for longer than the window → the watchdog routes through
    # the existing connection-close path. `liveness_timeout` telemetry is the
    # observable proof it fired.
    now = System.monotonic_time(:millisecond)
    state = state("ex_wedged", now - (window_ms() + 5_000))

    assert {:noreply, _} = ControlChannel.handle_info(:liveness_tick, state)

    assert_receive {:liveness_timeout, measurements, %{runner_id: "ex_wedged"} = metadata}, 500
    assert metadata.namespace == "liveness-watchdog-test-ns"
    assert measurements.silent_ms >= window_ms()
  end

  test "a frozen-but-connected runner within the window is not evicted" do
    # A brief freeze / a single missed keepalive ping leaves `last_seen` well
    # inside the window → no close, no telemetry, the watchdog just reschedules.
    now = System.monotonic_time(:millisecond)
    state = state("ex_ok", now - div(window_ms(), 3))

    assert {:noreply, ^state} = ControlChannel.handle_info(:liveness_tick, state)
    refute_receive {:liveness_timeout, _m, _meta}, 100
  end

  test "an inbound control_ping is the liveness signal — it bumps last_seen" do
    # Drive a real channel so the actual handler clause runs, then read state.
    write_fun = fn _ref, _payload -> :ok end
    {:ok, pid} = ControlChannel.start_link({"ns_live", "exec_live", :fake_ref, write_fun})
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid, :normal, 1_000) end)

    before = :sys.get_state(pid).last_seen
    Process.sleep(5)

    send(pid, {:flz_data, 1, "control_ping", "ka-nonce", %{}})
    # :sys.get_state is FIFO-ordered after the send, so the ping is processed.
    after_seen = :sys.get_state(pid).last_seen

    assert after_seen > before
  end
end
