defmodule Flezha.Plugin.Provider.WebsocketIdleTimeoutTest do
  @moduledoc """
  Watchdog contract tests for the WebSocket plugin's application-level
  idle-timeout reconnect.

  Failure mode this protects against: a WS goes silent for >30 s without a TCP
  error, leaving the consumer frozen on stale data — the shape a hibernating
  host produces, whose observed windows ran to 38 hours.
  """

  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Flezha.Plugin.Provider.Websocket
  alias FlezhaTest.Helper.NodeTestHelper
  alias Flezha.Process.Timer

  @telemetry_event [:flezha, :plugin, :websocket, :idle_reconnect]

  setup do
    parent = self()
    handler_id = "test-#{:erlang.unique_integer([:positive])}"

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

  defp base_state(overrides) do
    now = :erlang.monotonic_time(:millisecond)

    Map.merge(
      %{
        uri: "wss://test.local",
        plugin_id: "test_ws",
        post_connect: %{},
        overwatcher_pid: self(),
        payload_metadata: %{},
        idle_timeout_ms: 100,
        resubscribe_after_ms: 0,
        ignore_pattern: nil,
        websocket_established?: true,
        frames_seen: 0,
        last_frame_at: now,
        last_data_frame_at: now,
        last_resubscribe_at: now
      },
      Map.new(overrides)
    )
    |> FlezhaTest.Helper.WebsocketSourceFixtures.with_targets()
  end

  describe "build/1 — default and conversion" do
    test "defaults idle_timeout_ms to 30_000 when no flag is passed" do
      assert {:ok, %Websocket{idle_timeout_ms: 30_000}} =
               Websocket.build(%{"uri" => "wss://example.com"})
    end

    test "parses string idle_timeout_ms from CLI to integer" do
      assert {:ok, %Websocket{idle_timeout_ms: 45_000}} =
               Websocket.build(%{"uri" => "wss://example.com", "idle_timeout_ms" => "45000"})
    end

    test "clamps negative idle_timeout_ms to 0 (disables watchdog)" do
      assert {:ok, %Websocket{idle_timeout_ms: 0}} =
               Websocket.build(%{"uri" => "wss://example.com", "idle_timeout_ms" => "-5"})
    end

    test "falls back to default for non-integer idle_timeout_ms strings (runner typo)" do
      # An invalid value coerces to the safe default rather than crashing
      # the plugin manager mid-restart. A warning log is emitted; we don't
      # assert on it here because the value is what protects the user.
      assert {:ok, %Websocket{idle_timeout_ms: 30_000}} =
               Websocket.build(%{"uri" => "wss://example.com", "idle_timeout_ms" => "soon"})
    end
  end

  describe "handle_info(:idle_check) — silent past threshold" do
    test "exits with {:shutdown, :idle_timeout} when silence_ms >= idle_timeout_ms" do
      Process.flag(:trap_exit, true)
      parent = self()

      {pid, ref} =
        spawn_monitor(fn ->
          state =
            base_state(
              idle_timeout_ms: 100,
              last_frame_at: System.monotonic_time(:millisecond) - 250,
              overwatcher_pid: parent
            )

          Websocket.Client.handle_info(:idle_check, state)
          NodeTestHelper.park()
        end)

      assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :idle_timeout}}, 1_000
    end

    test "emits telemetry event with silence_ms measurement and plugin_id metadata" do
      parent = self()

      _ =
        spawn(fn ->
          state =
            base_state(
              idle_timeout_ms: 100,
              last_frame_at: System.monotonic_time(:millisecond) - 350,
              plugin_id: "exchange_trade_ws",
              overwatcher_pid: parent
            )

          Websocket.Client.handle_info(:idle_check, state)
        end)

      assert_receive {:telemetry, @telemetry_event, %{silence_ms: silence_ms},
                      %{plugin_id: "exchange_trade_ws"}},
                     1_000

      assert silence_ms >= 100
    end
  end

  describe "handle_info(:idle_check) — within threshold" do
    test "reschedules next tick at idle_timeout_ms / 2 when silence is below threshold" do
      state = base_state(idle_timeout_ms: 5_000)
      assert {:ok, ticked} = Websocket.Client.handle_info(:idle_check, state)
      assert Timer.armed?(ticked.timers, :idle_check)
      assert Map.delete(ticked, :timers) == Map.delete(state, :timers)
      # idle_timeout_ms / 2 = 2_500 ms; 1-second floor doesn't apply.
      assert Websocket.idle_tick_interval(state) == 2_500
    end

    test "tick interval has a 1-second floor for small idle_timeout_ms" do
      state = base_state(idle_timeout_ms: 100)
      assert {:ok, _} = Websocket.Client.handle_info(:idle_check, state)
      # idle_timeout_ms / 2 = 50 ms but the floor is 1_000 ms.
      assert Websocket.idle_tick_interval(state) == 1_000
    end

    test "stays alive (does NOT exit) when within threshold" do
      Process.flag(:trap_exit, true)
      parent = self()

      {pid, ref} =
        spawn_monitor(fn ->
          state = base_state(idle_timeout_ms: 5_000)
          Websocket.Client.handle_info(:idle_check, state)
          # Reached only when the check did not exit the process.
          send(parent, :checked)

          receive do
            :stop -> :ok
          end
        end)

      assert_receive :checked, 1_000
      refute_received {:DOWN, ^ref, :process, ^pid, {:shutdown, :idle_timeout}}
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000
    end
  end

  describe "handle_info(:idle_check) — disabled" do
    test "is a no-op when idle_timeout_ms is 0" do
      Process.flag(:trap_exit, true)
      parent = self()

      {pid, ref} =
        spawn_monitor(fn ->
          state =
            base_state(
              idle_timeout_ms: 0,
              last_data_frame_at: System.monotonic_time(:millisecond) - 60_000
            )

          Websocket.Client.handle_info(:idle_check, state)
          # Reached only when the check did not exit the process.
          send(parent, :checked)

          receive do
            :stop -> :ok
          end
        end)

      assert_receive :checked, 1_000
      refute_received {:DOWN, ^ref, :process, ^pid, {:shutdown, :idle_timeout}}
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000
    end
  end

  describe "handle_in/2 refreshes the watchdog" do
    test "updates last_data_frame_at to ~now on every data frame" do
      stale_ts = System.monotonic_time(:millisecond) - 60_000

      state =
        base_state(
          idle_timeout_ms: 1_000,
          last_data_frame_at: stale_ts,
          overwatcher_pid: spawn_dummy_overwatcher()
        )

      assert {:ok, new_state} = Websocket.Client.handle_in({:text, "trade"}, state)
      assert new_state.last_data_frame_at > stale_ts

      diff = System.monotonic_time(:millisecond) - new_state.last_data_frame_at
      assert diff >= 0 and diff < 50
    end
  end

  describe "handle_control/2 refreshes connection liveness but not data liveness" do
    test "a pong frame refreshes last_frame_at and leaves last_data_frame_at unchanged" do
      stale = System.monotonic_time(:millisecond) - 60_000
      state = base_state(last_frame_at: stale, last_data_frame_at: stale)

      assert {:ok, new_state} = Websocket.Client.handle_control({:pong, ""}, state)
      assert new_state.last_frame_at > stale
      assert new_state.last_data_frame_at == stale
    end

    test "a ping frame refreshes last_frame_at and leaves last_data_frame_at unchanged" do
      stale = System.monotonic_time(:millisecond) - 60_000
      state = base_state(last_frame_at: stale, last_data_frame_at: stale)

      assert {:ok, new_state} = Websocket.Client.handle_control({:ping, "irrelevant"}, state)
      assert new_state.last_frame_at > stale
      assert new_state.last_data_frame_at == stale
    end
  end

  describe "handle_connect/3 — fresh window on every (re)connect" do
    test "resets last_data_frame_at to now and schedules the first watchdog tick" do
      state =
        base_state(
          idle_timeout_ms: 100,
          last_data_frame_at: 0
        )

      before_ts = System.monotonic_time(:millisecond)
      assert {:ok, new_state} = Websocket.Client.handle_connect(101, [], state)
      after_ts = System.monotonic_time(:millisecond)

      assert new_state.last_data_frame_at >= before_ts
      assert new_state.last_data_frame_at <= after_ts

      # First watchdog tick is scheduled because idle_timeout_ms > 0.
      assert_receive :idle_check, 1_500
    end

    # The idle_timeout_ms: 0 counterpart lives in
    # WebsocketIdleTimeoutDisabledTest, which owns the disabled case.
  end

  describe "the idle window is never shorter than the ping that refreshes it" do
    setup do
      :ok = :logger.set_module_level(Websocket.Client, :all)
      on_exit(fn -> :logger.unset_module_level(Websocket.Client) end)
      :ok
    end

    defp window(idle_timeout_ms, ws_opts \\ []) do
      Websocket.Client.idle_window(%{
        idle_timeout_ms: idle_timeout_ms,
        ws_opts: ws_opts,
        plugin_id: "ws_floor_test"
      })
    end

    test "a window at the default ping interval is raised, and the applied value reported" do
      ping = Fresh.Option.ping_interval([])

      log = capture_log(fn -> assert window(ping) > ping end)

      assert log =~ "idle_timeout_ms=#{ping}"
      assert log =~ "applying #{ping * 2} ms"
    end

    test "the default idle window is exactly the default ping interval, so it is raised" do
      # The finding this floor closes: the two defaults were equal, so the
      # threshold was reached at the moment the next pong was due.
      ping = Fresh.Option.ping_interval([])
      assert Websocket.Client.default_idle_timeout_ms() == ping

      log =
        capture_log(fn -> assert window(Websocket.Client.default_idle_timeout_ms()) > ping end)

      assert log =~ "keepalive ping interval"
    end

    test "the floor follows the interval a user actually configured" do
      assert capture_log(fn -> assert window(20_000, ping_interval: 60_000) == 120_000 end) =~
               "keepalive ping interval 60000"
    end

    test "a window comfortably above the ping interval is left alone" do
      # `async: true` means the capture also sees other tests' output, so the
      # assertion is the absence of this sentence, never an empty capture.
      refute capture_log(fn -> assert window(90_000) == 90_000 end) =~ "keepalive ping interval"
    end

    test "zero still disables the watchdog — a decision, not a small window" do
      refute capture_log(fn -> assert window(0) == 0 end) =~ "keepalive ping interval"
    end
  end

  defp spawn_dummy_overwatcher do
    spawn(fn ->
      receive do
        _ -> :ok
      after
        2_000 -> :ok
      end
    end)
  end
end
