defmodule Flezha.Plugin.Provider.WebsocketIdleTimeoutDisabledTest do
  @moduledoc """
  Verifies that runners can fully disable the idle watchdog by passing
  `--Xidle_timeout_ms=0`. Without this opt-out, low-volume streams that
  legitimately go quiet for hours would reconnect every 30 s.

  Failure mode this protects against: the idle watchdog interfering with
  intentionally low-frequency WS subscriptions when a runner has
  declared the stream may stay quiet.
  """

  # async: false: global telemetry handlers.

  use ExUnit.Case, async: false

  alias Flezha.Plugin.Provider.Websocket

  @telemetry_event [:flezha, :ws, :idle_reconnect]

  setup do
    parent = self()
    handler_id = "test-disabled-#{:erlang.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler_id,
        @telemetry_event,
        fn event, measurements, metadata, _config ->
          send(parent, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok
  end

  test "build/1 with idle_timeout_ms=\"0\" disables the watchdog" do
    assert {:ok, %Websocket{idle_timeout_ms: 0}} =
             Websocket.build(%{"uri" => "wss://example.com", "idle_timeout_ms" => "0"})
  end

  test "no Process.exit and no telemetry event when watchdog is disabled, even after long silence" do
    Process.flag(:trap_exit, true)
    parent = self()

    stale = System.monotonic_time(:millisecond) - 60_000

    state = %{
      uri: "wss://test.local",
      plugin_id: "test_disabled_ws",
      post_connect: %{},
      overwatcher_pid: parent,
      payload_metadata: %{},
      idle_timeout_ms: 0,
      resubscribe_after_ms: 0,
      ignore_pattern: nil,
      websocket_established?: true,
      frames_seen: 0,
      last_frame_at: stale,
      last_data_frame_at: stale,
      last_resubscribe_at: stale
    }

    parent = self()

    {pid, ref} =
      spawn_monitor(fn ->
        Websocket.handle_info(:idle_check, state)
        # Reached only when the check did not exit the process.
        send(parent, :checked)

        receive do
          :stop -> :ok
        end
      end)

    assert_receive :checked, 1_000
    refute_received {:DOWN, ^ref, :process, ^pid, {:shutdown, :idle_timeout}}
    refute_received {:telemetry, @telemetry_event, _, _}

    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000
  end

  test "handle_connect/3 does not schedule any watchdog tick when disabled" do
    state = %{
      uri: "wss://test.local",
      plugin_id: "test_disabled_ws",
      post_connect: %{},
      overwatcher_pid: self(),
      payload_metadata: %{},
      idle_timeout_ms: 0,
      resubscribe_after_ms: 0,
      ignore_pattern: nil,
      websocket_established?: false,
      frames_seen: 0,
      last_frame_at: 0,
      last_data_frame_at: 0,
      last_resubscribe_at: 0
    }

    assert {:ok, _} = Websocket.handle_connect(101, [], state)

    # Both tiers off means there is no interval to arm at all.
    assert Websocket.idle_tick_interval(state) == nil
    refute_received :idle_check
  end
end
