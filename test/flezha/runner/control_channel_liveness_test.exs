defmodule FlezhaTest.Runner.ControlChannelLivenessTest do
  @moduledoc """
  Control-plane liveness watchdog.

  `Flezha.Runner.ControlChannel` stamps a monotonic `last_seen` on every
  inbound control frame — the SDK keepalive ping included — and a jittered
  watchdog treats sustained silence past the
  window as a connection close, routing through the SAME
  `handle_connection_closed/1` a real `{:flz_closed}` uses. This closes the UDS
  wedged-but-connected blind spot.

  These are deterministic handler-level tests (the watchdog decision is driven
  directly, like `ControlUnitHealthTest` drives `:heartbeat`): they never shrink
  the global window via `put_env`, which would falsely evict the suite's
  long-lived default-namespace runners.
  """
  use ExUnit.Case, async: true

  @moduletag :capture_log

  alias Flezha.Process.Timer

  alias Flezha.Runner.ControlChannel
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.Telemetry

  setup do
    Telemetry.capture([:flezha, :runner, :liveness_timeout],
      as: fn _event, measurements, metadata -> {:liveness_timeout, measurements, metadata} end
    )

    :ok
  end

  defp window_ms, do: Application.get_env(:flezha, :control_liveness_window_ms, 45_000)

  defp state(runner_id, last_seen) do
    %{
      namespace: "liveness-watchdog-test-ns",
      runner_id: runner_id,
      stream_ref: :fake_ref,
      write_fun: fn _ref, _payload_id, _message, _metadata -> :ok end,
      control_unit: nil,
      last_seen: last_seen,
      waiters: %{send_gate: MapSet.new()}
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

    assert {:noreply, rescheduled} = ControlChannel.handle_info(:liveness_tick, state)
    # Nothing changes but the tick the watchdog re-arms.
    assert Map.delete(rescheduled, :timers) == Map.delete(state, :timers)
    assert Timer.armed?(rescheduled.timers, :liveness)
    refute_received {:liveness_timeout, _m, _meta}
  end

  test "an inbound control_ping is the liveness signal — it bumps last_seen" do
    # Drive a real channel so the actual handler clause runs, then read state.
    write_fun = fn _ref, _payload_id, _message, _metadata -> :ok end
    {:ok, pid} = ControlChannel.start_link({"ns_live", "exec_live", :fake_ref, write_fun})
    on_exit(fn -> NodeTestHelper.ensure_stopped(pid, :normal, 1_000) end)

    before = :sys.get_state(pid).last_seen
    # `last_seen` is a millisecond stamp; the clock has to move for a bump to show.
    assert NodeTestHelper.wait_until(
             fn -> System.monotonic_time(:millisecond) > before end,
             1_000,
             1
           )

    send(pid, {:flz_data, 1, "control_ping", "ka-nonce", %{}})
    # :sys.get_state is FIFO-ordered after the send, so the ping is processed.
    after_seen = :sys.get_state(pid).last_seen

    assert after_seen > before
  end
end

defmodule Flezha.Runner.ControlChannelPingTest do
  @moduledoc """
  Controller side of the shared control-channel keepalive
  (self-healing-control-plane, Phase 8.1): an inbound `control_ping` is echoed
  back on the same dedicated payload so the SDK's steady-state idle watchdog
  re-arms, and it never enters the command path. A wedged controller stops
  echoing, so the runner's watchdog drives reconnect.
  """
  use ExUnit.Case, async: true

  alias Flezha.Runner.ControlChannel

  test "an inbound control_ping is echoed back verbatim on the control_ping payload" do
    test_pid = self()

    write_fun = fn stream_ref, payload_id, message, _metadata ->
      send(test_pid, {:wrote, stream_ref, payload_id, message})
      :ok
    end

    {:ok, pid} =
      ControlChannel.start_link({"ns_ping", "exec_ping", :fake_stream_ref, write_fun})

    # Opaque keepalive body — the SDK drops the echo's content after the
    # read-level re-arm, so the round-trip alone is what matters.
    ping_body = "ka-nonce-123"
    send(pid, {:flz_data, 1, "control_ping", ping_body, %{}})

    assert_receive {:wrote, :fake_stream_ref, payload_id, message}, 1000
    assert payload_id == "control_ping"
    assert message == ping_body

    GenServer.stop(pid)
  end

  test "a control_ping with no bound stream_ref is dropped without crashing" do
    test_pid = self()
    write_fun = fn _ref, _payload_id, _message, _metadata -> send(test_pid, :wrote) end

    {:ok, pid} =
      ControlChannel.start_link({"ns_ping2", "exec_ping2", nil, write_fun})

    send(pid, {:flz_data, 1, "control_ping", "x", %{}})

    # The ping has been handled once the channel answers a call.
    _ = :sys.get_state(pid)
    refute_received :wrote
    assert Process.alive?(pid)

    GenServer.stop(pid)
  end
end
