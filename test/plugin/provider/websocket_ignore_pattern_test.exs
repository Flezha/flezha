defmodule Flezha.Plugin.Provider.WebsocketIgnorePatternTest do
  @moduledoc """
  Watchdog contract tests for the WebSocket plugin's per-plugin
  `ignore_pattern` substring filter on `last_data_frame_at` updates.

  Failure mode this protects against: Kraken (or any) WS keeps emitting
  per-second `{"event":"heartbeat"}` JSON frames after the actual market
  feed dies. Without an `ignore_pattern`, those heartbeats refresh the
  watchdog timer indefinitely and the plugin manager never restarts the
  silent-data connection.
  """

  # fails when: heartbeat-only frames mask data silence and the watchdog never fires

  use ExUnit.Case, async: false

  alias Flezha.Plugin.Provider.Websocket

  defp base_state(overrides) do
    Map.merge(
      %{
        uri: "wss://test.local",
        plugin_id: "test_ws",
        post_connect: %{},
        overwatcher_pid: spawn_dummy_overwatcher(),
        payload_metadata: %{},
        idle_timeout_ms: 100,
        ignore_pattern: nil,
        last_data_frame_at: System.monotonic_time(:millisecond)
      },
      Map.new(overrides)
    )
  end

  describe "build/1 — :ignore_pattern parsing" do
    test "defaults ignore_pattern to nil when no flag is passed" do
      assert {:ok, %Websocket{ignore_pattern: nil}} =
               Websocket.build(%{"uri" => "wss://example.com"})
    end

    test "stores a string ignore_pattern verbatim" do
      assert {:ok, %Websocket{ignore_pattern: "\"event\":\"heartbeat\""}} =
               Websocket.build(%{
                 "uri" => "wss://example.com",
                 "ignore_pattern" => "\"event\":\"heartbeat\""
               })
    end

    test "treats empty string as no pattern (executor unsetting via env override)" do
      assert {:ok, %Websocket{ignore_pattern: nil}} =
               Websocket.build(%{"uri" => "wss://example.com", "ignore_pattern" => ""})
    end
  end

  describe "handle_in/2 — heartbeat suppression with ignore_pattern set" do
    test "text frame matching ignore_pattern does NOT refresh last_data_frame_at" do
      stale_ts = System.monotonic_time(:millisecond) - 60_000

      state =
        base_state(
          last_data_frame_at: stale_ts,
          ignore_pattern: "\"event\":\"heartbeat\""
        )

      assert {:ok, new_state} =
               Websocket.handle_in({:text, "{\"event\":\"heartbeat\"}"}, state)

      assert new_state.last_data_frame_at == stale_ts
      assert Map.fetch!(new_state, :frames_seen) == 1
    end

    test "text frame NOT matching ignore_pattern refreshes last_data_frame_at" do
      stale_ts = System.monotonic_time(:millisecond) - 60_000

      state =
        base_state(
          last_data_frame_at: stale_ts,
          ignore_pattern: "\"event\":\"heartbeat\""
        )

      assert {:ok, new_state} =
               Websocket.handle_in({:text, "[{\"price\":\"42000.00\"}]"}, state)

      assert new_state.last_data_frame_at > stale_ts
    end

    test "binary frame always refreshes the timer regardless of ignore_pattern" do
      stale_ts = System.monotonic_time(:millisecond) - 60_000

      state =
        base_state(
          last_data_frame_at: stale_ts,
          ignore_pattern: "heartbeat"
        )

      # Binary payload happens to contain "heartbeat" bytes — must still
      # be treated as data, since the pattern is text-shape only.
      assert {:ok, new_state} = Websocket.handle_in({:binary, "heartbeat"}, state)
      assert new_state.last_data_frame_at > stale_ts
    end
  end

  describe "handle_in/2 — default behaviour without ignore_pattern (regression guard)" do
    test "text frame with no ignore_pattern refreshes timer (pre-change behaviour)" do
      stale_ts = System.monotonic_time(:millisecond) - 60_000

      state =
        base_state(
          last_data_frame_at: stale_ts,
          ignore_pattern: nil
        )

      assert {:ok, new_state} =
               Websocket.handle_in({:text, "{\"event\":\"heartbeat\"}"}, state)

      # Without an ignore_pattern, even a heartbeat-shaped frame counts
      # as data — preserving the pre-change semantics.
      assert new_state.last_data_frame_at > stale_ts
    end
  end

  describe "watchdog drives reconnect on heartbeat-only stream" do
    test "heartbeat-only frames over (idle_timeout_ms + tick_interval) trigger {:shutdown, :idle_timeout}" do
      Process.flag(:trap_exit, true)
      parent = self()

      {pid, ref} =
        spawn_monitor(fn ->
          state =
            base_state(
              idle_timeout_ms: 100,
              ignore_pattern: "\"event\":\"heartbeat\"",
              overwatcher_pid: parent
            )

          # Drive 5 heartbeats over ~250 ms — none should bump the timer.
          state =
            Enum.reduce(1..5, state, fn _, acc ->
              Process.sleep(50)
              {:ok, new_state} = Websocket.handle_in({:text, "{\"event\":\"heartbeat\"}"}, acc)
              new_state
            end)

          # Now silence_ms (since `last_data_frame_at`) is well past
          # idle_timeout_ms — handle_info(:idle_check) MUST exit.
          Websocket.handle_info(:idle_check, state)
          Process.sleep(500)
        end)

      assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :idle_timeout}}, 1_500
    end

    test "non-heartbeat frame in middle of heartbeat stream KEEPS connection alive" do
      Process.flag(:trap_exit, true)
      parent = self()

      {pid, ref} =
        spawn_monitor(fn ->
          state =
            base_state(
              idle_timeout_ms: 200,
              ignore_pattern: "\"event\":\"heartbeat\"",
              overwatcher_pid: parent
            )

          # Heartbeat → heartbeat → real data → heartbeat. The real data
          # frame mid-stream resets the timer; subsequent heartbeats can't
          # re-stale it within idle_timeout_ms.
          state =
            [
              "{\"event\":\"heartbeat\"}",
              "{\"event\":\"heartbeat\"}",
              "[{\"price\":\"42000.00\"}]",
              "{\"event\":\"heartbeat\"}"
            ]
            |> Enum.reduce(state, fn frame, acc ->
              Process.sleep(40)
              {:ok, new_state} = Websocket.handle_in({:text, frame}, acc)
              new_state
            end)

          # silence_ms ≈ 40 ms (one tick after the real data frame), which
          # is well below idle_timeout_ms — handle_info(:idle_check) must
          # NOT exit.
          {:ok, _} = Websocket.handle_info(:idle_check, state)

          # Live to tell the tale.
          send(parent, :survived)

          receive do
            :stop -> :ok
          after
            1_000 -> :ok
          end
        end)

      assert_receive :survived, 1_500
      refute_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :idle_timeout}}, 200

      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000
    end
  end

  defp spawn_dummy_overwatcher do
    spawn(fn ->
      receive do
        _ -> :ok
      after
        5_000 -> :ok
      end
    end)
  end
end
