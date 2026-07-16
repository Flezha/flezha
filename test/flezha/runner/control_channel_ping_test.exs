defmodule Flezha.Runner.ControlChannelPingTest do
  @moduledoc """
  Controller side of the shared control-channel keepalive
  (self-healing-control-plane, Phase 8.1): an inbound `control_ping` is echoed
  back on the same dedicated payload so the SDK's steady-state idle watchdog
  re-arms, and it never enters the command path. A wedged controller stops
  echoing, so the runner's watchdog drives reconnect.
  """
  use ExUnit.Case, async: false

  alias Flezha.Runner.ControlChannel
  alias Flezha.Service.Payloader

  test "an inbound control_ping is echoed back verbatim on the control_ping payload" do
    test_pid = self()

    write_fun = fn stream_ref, payload ->
      send(test_pid, {:wrote, stream_ref, payload})
      :ok
    end

    {:ok, pid} =
      ControlChannel.start_link({"ns_ping", "exec_ping", :fake_stream_ref, write_fun})

    # Opaque keepalive body — the SDK drops the echo's content after the
    # read-level re-arm, so the round-trip alone is what matters.
    ping_body = "ka-nonce-123"
    send(pid, {:flz_data, 1, "control_ping", ping_body, %{}})

    assert_receive {:wrote, :fake_stream_ref, payload}, 1000
    assert payload == Payloader.build_from_bin("control_ping", ping_body)

    GenServer.stop(pid)
  end

  test "a control_ping with no bound stream_ref is dropped without crashing" do
    test_pid = self()
    write_fun = fn _ref, _payload -> send(test_pid, :wrote) end

    {:ok, pid} =
      ControlChannel.start_link({"ns_ping2", "exec_ping2", nil, write_fun})

    send(pid, {:flz_data, 1, "control_ping", "x", %{}})

    refute_receive :wrote, 200
    assert Process.alive?(pid)

    GenServer.stop(pid)
  end
end
