defmodule Flezha.Plugin.Provider.WebsocketIgnorePatternTest do
  @moduledoc """
  Watchdog contract tests for the WebSocket plugin's per-plugin
  `ignore_pattern` substring filter on `last_data_frame_at` updates.

  Failure mode this protects against: an upstream WS keeps emitting
  per-second `{"event":"heartbeat"}` JSON frames after the actual market
  feed dies. Without an `ignore_pattern`, those heartbeats refresh the
  watchdog timer indefinitely and the plugin manager never restarts the
  silent-data connection.
  """

  # fails when: heartbeat-only frames mask data silence and the watchdog never fires

  use ExUnit.Case, async: true

  alias Flezha.Plugin.Provider.Websocket

  defp base_state(overrides) do
    now = :erlang.monotonic_time(:millisecond)

    Map.merge(
      %{
        uri: "wss://test.local",
        plugin_id: "test_ws",
        post_connect: %{},
        overwatcher_pid: spawn_dummy_overwatcher(),
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

    test "treats empty string as no pattern (runner unsetting via env override)" do
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
               Websocket.Client.handle_in({:text, "{\"event\":\"heartbeat\"}"}, state)

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
               Websocket.Client.handle_in({:text, "[{\"price\":\"42000.00\"}]"}, state)

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
      assert {:ok, new_state} = Websocket.Client.handle_in({:binary, "heartbeat"}, state)
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
               Websocket.Client.handle_in({:text, "{\"event\":\"heartbeat\"}"}, state)

      # Without an ignore_pattern, even a heartbeat-shaped frame counts
      # as data — preserving the pre-change semantics.
      assert new_state.last_data_frame_at > stale_ts
    end
  end

  describe "handle_in/2 — connection-liveness refresh (regression guard)" do
    test "a heartbeat frame refreshes last_frame_at even though it is not data" do
      stale = :erlang.monotonic_time(:millisecond) - 60_000

      state =
        base_state(
          ignore_pattern: "\"event\":\"heartbeat\"",
          last_frame_at: stale,
          last_data_frame_at: stale
        )

      assert {:ok, new_state} =
               Websocket.Client.handle_in({:text, "{\"event\":\"heartbeat\"}"}, state)

      # Connection liveness advances on any frame; data liveness does not
      # advance on a heartbeat. This split is the core fix that stops the
      # watchdog false-killing a quiet-but-alive feed.
      assert new_state.last_frame_at > stale
      assert new_state.last_data_frame_at == stale
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
